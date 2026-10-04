package eu.kanade.domain.source.service

import eu.kanade.tachiyomi.source.ProgressAwareSource
import eu.kanade.tachiyomi.source.model.ChapterProgress
import eu.kanade.tachiyomi.source.model.SManga
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import logcat.LogPriority
import tachiyomi.core.common.util.system.logcat
import tachiyomi.core.common.preference.PreferenceStore
import tachiyomi.domain.chapter.interactor.UpdateChapter
import tachiyomi.domain.chapter.model.Chapter
import tachiyomi.domain.chapter.model.ChapterUpdate
import tachiyomi.domain.source.service.SourceProgressPolicy
import uy.kohesive.injekt.Injekt
import uy.kohesive.injekt.api.get
import java.util.concurrent.ConcurrentHashMap

/**
 * Host-side coordinator for [ProgressAwareSource].
 *
 * Pull: short-timeout fetch, merge with local via [SourceProgressPolicy].
 * Push: offline queue keyed by (mangaId, chapterUrl); newer records overwrite;
 *       persisted across process death via [PreferenceStore].
 */
class SourceProgressSync(
    private val updateChapter: UpdateChapter,
    private val preferenceStore: PreferenceStore = Injekt.get(),
    private val sourcePreferences: SourcePreferences = Injekt.get(),
) {
    private val queueMutex = Mutex()
    private val pendingPush = ConcurrentHashMap<Long, ConcurrentHashMap<String, ChapterProgress>>()
    private val snapshotCache = ConcurrentHashMap<Long, Map<String, ChapterProgress>>()
    private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }
    private val queuePref by lazy {
        preferenceStore.getString(QUEUE_PREF_KEY, "{}")
    }
    @Volatile
    private var loaded = false

    private suspend fun ensureLoaded() {
        if (loaded) return
        queueMutex.withLock {
            if (loaded) return@withLock
            runCatching {
                val raw = queuePref.get()
                if (raw.isBlank() || raw == "{}") {
                    loaded = true
                    return@withLock
                }
                val stored = json.decodeFromString<Map<String, Map<String, StoredProgress>>>(raw)
                pendingPush.clear()
                stored.forEach { (mangaIdStr, chapters) ->
                    val mangaId = mangaIdStr.toLongOrNull() ?: return@forEach
                    val map = ConcurrentHashMap<String, ChapterProgress>()
                    chapters.forEach { (url, sp) ->
                        map[url] = sp.toChapterProgress(url)
                    }
                    if (map.isNotEmpty()) pendingPush[mangaId] = map
                }
            }.onFailure { e ->
                logcat(LogPriority.ERROR, e) { "Failed to load source progress offline queue" }
            }
            loaded = true
        }
    }

    private fun persistQueue() {
        val snapshot = pendingPush.mapKeys { it.key.toString() }.mapValues { (_, chapters) ->
            chapters.mapValues { (_, p) -> StoredProgress.from(p) }
        }
        queuePref.set(json.encodeToString(snapshot))
    }

    fun allowBackwards(): Boolean = sourcePreferences.allowSourceProgressBackwards().get()

    /**
     * Pull progress for a single chapter before opening the reader.
     * @return merged 0-based page index to start at
     */
    suspend fun pullForReaderOpen(
        source: ProgressAwareSource,
        manga: SManga,
        mangaId: Long,
        chapter: Chapter,
        localPageIndex: Int,
        localCompleted: Boolean,
        timeoutMs: Long = 1500L,
        allowBackwards: Boolean = allowBackwards(),
    ): Int {
        ensureLoaded()
        val remote = withTimeoutOrNull(timeoutMs) {
            // Always hit the network for the chapter being opened so a lower remote
            // page (allow-backwards) is not masked by a stale series snapshot.
            source.getProgress(manga, listOf(chapter.url)).firstOrNull()
                ?: snapshotCache[mangaId]?.let { cache ->
                    cache[chapter.url] ?: cache[chapter.url.substringBefore("?")]
                }
        }
        val merged = SourceProgressPolicy.merge(
            localPageIndex = localPageIndex,
            localCompleted = localCompleted,
            remote = remote,
            allowBackwards = allowBackwards,
        )
        if (merged.acceptRemote && chapter.id != null) {
            updateChapter.await(
                ChapterUpdate(
                    id = chapter.id,
                    lastPageRead = merged.pageIndex.toLong(),
                    read = merged.completed,
                ),
            )
            // Stale local progress must not be flushed back over the accepted remote
            dropQueued(mangaId, chapter.url)
        }
        if (merged.schedulePush) {
            enqueue(
                mangaId,
                ChapterProgress(
                    chapterUrl = chapter.url,
                    pageIndex = localPageIndex,
                    completed = localCompleted,
                    readAt = System.currentTimeMillis(),
                ),
            )
        }
        return merged.pageIndex
    }

    /**
     * Pull interesting chapters when opening / refreshing the series screen.
     * Prefers [ProgressAwareSource.getProgressSnapshot] (Kavita: series volumes pagesRead).
     */
    suspend fun pullForSeriesOpen(
        source: ProgressAwareSource,
        manga: SManga,
        mangaId: Long,
        chapters: List<Chapter>,
        allowBackwards: Boolean = allowBackwards(),
    ) {
        ensureLoaded()
        val interesting = chapters.filter { !it.read || it.lastPageRead > 0 }
        if (interesting.isEmpty() && chapters.isEmpty()) return

        val snapshot = runCatching { source.getProgressSnapshot(manga) }.getOrNull()
        if (snapshot != null) {
            snapshotCache[mangaId] = snapshot
        }

        // When we have a snapshot, merge against all chapters that appear in it (or interesting set)
        val targets = if (snapshot != null) {
            chapters.filter { ch ->
                snapshot.containsKey(ch.url) || snapshot.containsKey(ch.url.substringBefore("?")) ||
                    !ch.read || ch.lastPageRead > 0
            }
        } else {
            interesting
        }
        if (targets.isEmpty()) return

        val remoteByUrl = snapshot
            ?: runCatching {
                source.getProgress(manga, targets.map { it.url }).associateBy { it.chapterUrl }
            }.getOrDefault(emptyMap())

        val updates = targets.mapNotNull { chapter ->
            val remote = remoteByUrl[chapter.url]
                ?: remoteByUrl[chapter.url.substringBefore("?")]
                ?: remoteByUrl[chapter.url.substringBefore("?").substringBefore("_")]
                ?: remoteByUrl.entries.firstOrNull { (k, _) ->
                    val base = chapter.url.substringBefore("?").substringBefore("_")
                    k.substringBefore("?").substringBefore("_") == base
                }?.value
                ?: return@mapNotNull null
            val merged = SourceProgressPolicy.merge(
                localPageIndex = chapter.lastPageRead.toInt(),
                localCompleted = chapter.read,
                remote = remote,
                allowBackwards = allowBackwards,
            )
            if (!merged.acceptRemote || chapter.id == null) {
                if (merged.schedulePush) {
                    enqueue(
                        mangaId,
                        ChapterProgress(
                            chapterUrl = chapter.url,
                            pageIndex = chapter.lastPageRead.toInt(),
                            completed = chapter.read,
                            readAt = System.currentTimeMillis(),
                        ),
                    )
                }
                return@mapNotNull null
            }
            dropQueued(mangaId, chapter.url)
            ChapterUpdate(
                id = chapter.id,
                lastPageRead = merged.pageIndex.toLong(),
                read = merged.completed,
            )
        }
        if (updates.isNotEmpty()) {
            updateChapter.awaitAll(updates)
        }
    }

    /**
     * Manual / series-screen sync: push pending then pull snapshot.
     */
    suspend fun syncSeries(
        source: ProgressAwareSource,
        manga: SManga,
        mangaId: Long,
        chapters: List<Chapter>,
    ) {
        ensureLoaded()
        // Pull first so allow-backwards can accept a lower remote page *before*
        // we flush any stale offline queue entries that would overwrite it.
        pullForSeriesOpen(source, manga, mangaId, chapters)
        flush(source, manga, mangaId)
    }

    /** Remove a pending push so an accepted remote value is not overwritten on flush. */
    private suspend fun dropQueued(mangaId: Long, chapterUrl: String) {
        queueMutex.withLock {
            val map = pendingPush[mangaId] ?: return@withLock
            map.remove(chapterUrl)
            map.remove(chapterUrl.substringBefore("?"))
            if (map.isEmpty()) pendingPush.remove(mangaId)
            persistQueue()
        }
    }

    suspend fun enqueue(mangaId: Long, progress: ChapterProgress) {
        ensureLoaded()
        queueMutex.withLock {
            pendingPush.getOrPut(mangaId) { ConcurrentHashMap() }[progress.chapterUrl] = progress
            persistQueue()
        }
    }

    suspend fun flush(source: ProgressAwareSource, manga: SManga, mangaId: Long) {
        ensureLoaded()
        val batch = queueMutex.withLock {
            pendingPush.remove(mangaId)?.values?.toList().orEmpty().also {
                if (it.isNotEmpty()) persistQueue()
            }
        }
        if (batch.isEmpty()) return
        try {
            source.updateProgress(manga, batch)
            snapshotCache.remove(mangaId)
        } catch (e: Exception) {
            logcat(LogPriority.ERROR, e) { "Failed to push source progress; re-queueing" }
            queueMutex.withLock {
                val map = pendingPush.getOrPut(mangaId) { ConcurrentHashMap() }
                batch.forEach { map[it.chapterUrl] = it }
                persistQueue()
            }
        }
    }

    fun invalidateSnapshot(mangaId: Long) {
        snapshotCache.remove(mangaId)
    }

    @Serializable
    private data class StoredProgress(
        val pageIndex: Int,
        val pageCount: Int? = null,
        val completed: Boolean = false,
        val readAt: Long = 0L,
    ) {
        fun toChapterProgress(url: String) = ChapterProgress(
            chapterUrl = url,
            pageIndex = pageIndex,
            pageCount = pageCount,
            completed = completed,
            readAt = readAt,
        )

        companion object {
            fun from(p: ChapterProgress) = StoredProgress(
                pageIndex = p.pageIndex,
                pageCount = p.pageCount,
                completed = p.completed,
                readAt = p.readAt,
            )
        }
    }

    companion object {
        private const val QUEUE_PREF_KEY = "source_progress_offline_queue"
    }
}

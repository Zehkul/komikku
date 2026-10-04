package eu.kanade.tachiyomi.source

import eu.kanade.tachiyomi.source.model.ChapterProgress
import eu.kanade.tachiyomi.source.model.SManga

/**
 * Optional capability for sources that store reading progress on their own server
 * (Kavita, Komga, Suwayomi, …).
 *
 * Default methods are no-ops so ordinary sources are unaffected.
 * Host policy (when to push/pull, conflict rules, offline queue) lives in the app.
 */
interface ProgressAwareSource {

    /**
     * Push one or more progress records.
     * Must be idempotent. Later records for the same chapterUrl supersede earlier ones.
     * May be called with a batch; the source may collapse them into a single network call.
     */
    suspend fun updateProgress(manga: SManga, progress: List<ChapterProgress>) {}

    /**
     * Pull progress for the given chapter URLs.
     * Return only chapters the source knows about. Empty list = nothing known.
     */
    suspend fun getProgress(manga: SManga, chapterUrls: List<String>): List<ChapterProgress> =
        emptyList()

    /**
     * Optional bulk snapshot. Return null to prefer per-chapter fetches.
     */
    suspend fun getProgressSnapshot(manga: SManga): Map<String, ChapterProgress>? = null
}

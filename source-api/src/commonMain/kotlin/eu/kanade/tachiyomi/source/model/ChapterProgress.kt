package eu.kanade.tachiyomi.source.model

/**
 * Page-level reading progress for a single chapter, owned by the source.
 *
 * @param chapterUrl Exact [SChapter.url] this progress belongs to.
 * @param pageIndex 0-based index of the last page the user has reached.
 * @param pageCount Total pages in the chapter, if known.
 * @param completed Whether the chapter is considered fully read.
 * @param readAt Epoch milliseconds of the last progress update; 0 = unknown.
 */
data class ChapterProgress(
    val chapterUrl: String,
    val pageIndex: Int,
    val pageCount: Int? = null,
    val completed: Boolean = false,
    val readAt: Long = 0L,
)

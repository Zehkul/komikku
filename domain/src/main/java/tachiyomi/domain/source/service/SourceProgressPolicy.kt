package tachiyomi.domain.source.service

import eu.kanade.tachiyomi.source.model.ChapterProgress

/**
 * Host-side conflict rule for source-owned page progress.
 *
 * - Remote page ahead → accept remote.
 * - Local page ahead + [allowBackwards] → accept remote (other client went back).
 * - Local page ahead + !allowBackwards → keep local and schedule push.
 * - Explicit local unread (page 0 + not completed) → push unread.
 */
object SourceProgressPolicy {

    fun merge(
        localPageIndex: Int,
        localCompleted: Boolean,
        remote: ChapterProgress?,
        allowBackwards: Boolean = false,
    ): MergeResult {
        if (remote == null) {
            return MergeResult(
                pageIndex = localPageIndex,
                completed = localCompleted,
                acceptRemote = false,
                schedulePush = localPageIndex > 0 || localCompleted,
            )
        }

        val remotePage = remote.pageIndex
        val remoteCompleted = remote.completed

        return when {
            remotePage > localPageIndex -> MergeResult(
                pageIndex = remotePage,
                completed = remoteCompleted || localCompleted,
                acceptRemote = true,
                schedulePush = false,
            )

            remotePage < localPageIndex -> {
                if (allowBackwards) {
                    // Source went backwards (web UI, another client) – trust it
                    MergeResult(
                        pageIndex = remotePage,
                        completed = remoteCompleted,
                        acceptRemote = true,
                        schedulePush = false,
                    )
                } else {
                    MergeResult(
                        pageIndex = localPageIndex,
                        completed = localCompleted,
                        acceptRemote = false,
                        schedulePush = true,
                    )
                }
            }

            // Same page index
            remoteCompleted && !localCompleted -> {
                if (localPageIndex == 0) {
                    MergeResult(
                        pageIndex = 0,
                        completed = false,
                        acceptRemote = false,
                        schedulePush = true,
                    )
                } else {
                    MergeResult(
                        pageIndex = localPageIndex,
                        completed = true,
                        acceptRemote = true,
                        schedulePush = false,
                    )
                }
            }

            !remoteCompleted && localCompleted -> {
                if (allowBackwards) {
                    MergeResult(
                        pageIndex = remotePage,
                        completed = false,
                        acceptRemote = true,
                        schedulePush = false,
                    )
                } else {
                    MergeResult(
                        pageIndex = localPageIndex,
                        completed = true,
                        acceptRemote = false,
                        schedulePush = true,
                    )
                }
            }

            else -> MergeResult(
                pageIndex = localPageIndex,
                completed = localCompleted,
                acceptRemote = false,
                schedulePush = false,
            )
        }
    }

    data class MergeResult(
        val pageIndex: Int,
        val completed: Boolean,
        val acceptRemote: Boolean,
        val schedulePush: Boolean,
    )
}

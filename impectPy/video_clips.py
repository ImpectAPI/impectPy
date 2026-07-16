# load packages
import shutil
import subprocess
import tempfile
import warnings
from pathlib import Path

import pandas as pd

from impectPy.helpers import RateLimitedAPI, ImpectSession, ForbiddenError
from .data import getDataFromHost

######
#
# This function cuts a short video clip around each event of an event dataframe
# and merges them into a single video file.
#
# NOTE: This is an interim solution. The video API currently serves full match
# clips that have to be cut client-side using ffmpeg. This will most likely be
# replaced by an API endpoint that returns already-cut clips.
#
######

# columns the events dataframe must provide to build the clip windows
REQUIRED_COLUMNS = ["matchId", "gameTimeInSec", "duration"]


def _normalize_filter(target_width: int, target_height: int, target_fps: int) -> str:
    """Build the ffmpeg filter that normalizes every clip to a common output spec.

    Clips cut from different match videos may differ in resolution/fps. Normalizing
    them to identical dimensions, aspect ratio and frame rate lets the individual
    clips be concatenated with a cheap stream copy afterwards.
    """
    return (
        f"scale={target_width}:{target_height}:force_original_aspect_ratio=decrease,"
        f"pad={target_width}:{target_height}:(ow-iw)/2:(oh-ih)/2,"
        f"setsar=1,fps={target_fps}"
    )


def _run_ffmpeg(args: list) -> None:
    """Run ffmpeg quietly; raise CalledProcessError (with stderr) on failure."""
    subprocess.run(
        ["ffmpeg", "-hide_banner", "-loglevel", "error", "-nostats", "-y", *args],
        capture_output=True,
        text=True,
        check=True,
    )


def getVideoClips(
        events: pd.DataFrame,
        output_path: str,
        token: str,
        lead: int = 3,
        lag: int = 3,
        target_width: int = 1920,
        target_height: int = 1080,
        target_fps: int = 25,
        warn_above_seconds: float = 600,
        session: ImpectSession = ImpectSession()
) -> Path:
    """Cut and merge a video clip for each row of an events DataFrame (interim, client-side).

    For every row of the (pre-filtered) ``events`` DataFrame a clip window is built as
    ``gameTimeInSec - lead`` to ``gameTimeInSec + duration + lag``, the corresponding
    video is fetched from the Impect API and cut with ffmpeg, and all clips are merged
    into a single file at ``output_path``. Input row order is preserved.

    Events whose match video is not available to the user (HTTP 403) are skipped with a
    warning; an exception is only raised if none of the requested clips are available.

    Requires the ``ffmpeg`` binary to be installed and available on ``PATH``.
    """
    # create an instance of RateLimitedAPI
    connection = RateLimitedAPI(session)

    # construct header with access token
    connection.session.headers.update({"Authorization": f"Bearer {token}"})

    return getVideoClipsFromHost(
        events, output_path, lead, lag, target_width, target_height, target_fps,
        warn_above_seconds, connection, "https://api.impect.com"
    )


def getVideoClipsFromHost(
        events: pd.DataFrame,
        output_path: str,
        lead: int,
        lag: int,
        target_width: int,
        target_height: int,
        target_fps: int,
        warn_above_seconds: float,
        connection: RateLimitedAPI,
        host: str
) -> Path:
    """Cut a clip per event from the given host's match videos and merge them into one file.

    Clips whose video is forbidden to the user (HTTP 403) are skipped with a warning; an
    exception is raised only if no clips could be created at all.
    """
    # warn that this is an interim, client-side solution
    warnings.warn(
        "getVideoClips() cuts full match clips client-side using ffmpeg. This is an "
        "interim solution and will most likely be replaced by an API endpoint that "
        "returns already-cut clips.",
        UserWarning,
        stacklevel=2
    )

    # ensure ffmpeg is available before doing any work
    if shutil.which("ffmpeg") is None:
        raise RuntimeError(
            "ffmpeg was not found on PATH. getVideoClips() requires ffmpeg to be "
            "installed (see https://ffmpeg.org/download.html)."
        )

    # validate input dataframe
    if events is None or len(events) == 0:
        raise ValueError("The provided events DataFrame is empty.")

    missing_columns = [col for col in REQUIRED_COLUMNS if col not in events.columns]
    if len(missing_columns) > 0:
        raise ValueError(
            f"The provided events DataFrame is missing required column(s): "
            f"{', '.join(missing_columns)}."
        )

    # build clip windows, preserving the input row order
    clips = events[REQUIRED_COLUMNS].copy()
    clips["startTime"] = clips["gameTimeInSec"] - lead
    clips["endTime"] = clips["gameTimeInSec"] + clips["duration"] + lag

    # warn (do not block) if the expected total output length is large, as fetching
    # and re-encoding scales with the total number of seconds of video
    expected_seconds = float((clips["duration"] + lead + lag).sum())
    if expected_seconds > warn_above_seconds:
        warnings.warn(
            f"The requested clips add up to ~{expected_seconds / 60:.1f} minutes of "
            f"video across {len(clips)} event(s). Fetching and re-encoding may be slow "
            f"and produce a large file. Raise 'warn_above_seconds' to silence this warning.",
            UserWarning,
            stacklevel=2
        )

    # ensure the output directory exists
    final_file = Path(output_path)
    if final_file.parent != Path(""):
        final_file.parent.mkdir(parents=True, exist_ok=True)

    normalize_filter = _normalize_filter(target_width, target_height, target_fps)

    # cut every clip into its own normalized file inside a temporary directory so the
    # working directory is never polluted and cleanup is automatic
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        clip_files = []
        skipped = []
        forbidden_matches = set()

        try:
            for i, clip in enumerate(clips.to_dict("records")):
                match_id = clip["matchId"]

                # once a match is known to be forbidden, skip its remaining clips without
                # re-requesting or warning again
                if match_id in forbidden_matches:
                    skipped.append(match_id)
                    continue

                # get the video url and the actual video timestamps for this clip's match
                try:
                    video = getDataFromHost(
                        url=(
                            f"{host}/v5/customerapi/matches/{match_id}/videos"
                            f"?start={clip['startTime']}&end={clip['endTime']}"
                        ),
                        method="GET",
                        connection=connection,
                    )
                except ForbiddenError:
                    # the user has no access to this match's video -> warn once and skip all
                    # of its clips instead of aborting the whole reel
                    warnings.warn(
                        f"The video for match {match_id} is not available to this user "
                        f"(HTTP 403); skipping all clips from this match.",
                        UserWarning,
                        stacklevel=2
                    )
                    forbidden_matches.add(match_id)
                    skipped.append(match_id)
                    continue

                if video is None or len(video) == 0:
                    raise RuntimeError(
                        f"No video returned for match {clip['matchId']} between "
                        f"{clip['startTime']}s and {clip['endTime']}s."
                    )

                # extract data
                video_url = video.iloc[0]["url"]
                video_start_time = video.iloc[0]["timestampsStartVideoTimeInSec"]
                video_end_time = video.iloc[0]["timestampsEndVideoTimeInSec"]

                clip_file = tmp_path / f"clip_{i}.mp4"

                # re-encode + normalize so clips from different videos share one format
                _run_ffmpeg(
                    [
                        "-ss", str(video_start_time),
                        "-to", str(video_end_time),
                        "-i", video_url,
                        "-vf", normalize_filter,
                        "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
                        "-pix_fmt", "yuv420p",
                        "-c:a", "aac", "-ar", "48000", "-ac", "2",
                        str(clip_file),
                    ]
                )

                clip_files.append(clip_file)

            # raise if no clip could be created because every video was unavailable
            if not clip_files:
                raise Exception(
                    f"No video clips could be created: all {len(clips)} requested "
                    f"clip(s) were unavailable to this user."
                )

            # merge the clips into the final output file
            final_file.unlink(missing_ok=True)

            if len(clip_files) == 1:
                # single clip: just move it to the final destination, nothing to merge
                shutil.move(str(clip_files[0]), str(final_file))
            else:
                # multiple clips share one format -> stream copy is safe
                concat_file = tmp_path / "clips.txt"
                concat_file.write_text(
                    "".join(f"file '{f.as_posix()}'\n" for f in clip_files)
                )
                _run_ffmpeg(
                    [
                        "-f", "concat", "-safe", "0", "-i", str(concat_file),
                        "-c", "copy", str(final_file),
                    ]
                )

            if skipped:
                print(
                    f"Created {final_file} from {len(clip_files)} clip(s); "
                    f"skipped {len(skipped)} unavailable clip(s)"
                )
            else:
                print(f"Created {final_file} from {len(clip_files)} clip(s)")

        except subprocess.CalledProcessError as e:
            # surface ffmpeg's own error output to help debugging
            raise RuntimeError(f"ffmpeg failed:\n{e.stderr}") from e

    return final_file

from urllib.parse import parse_qs, urlparse

from loguru import logger
from youtube_transcript_api import NoTranscriptFound, TranscriptsDisabled, YouTubeTranscriptApi
from youtube_transcript_api.formatters import TextFormatter

from llm_engineering.domain.documents import VideoDocument

from .base import BaseCrawler


class YoutubeCrawler(BaseCrawler):
    model = VideoDocument

    def __init__(self, languages: list[str] | None = None) -> None:
        super().__init__()
        self._languages = languages or ["es", "en"]
        self._api = YouTubeTranscriptApi()

    def extract(self, link: str, **kwargs) -> None:
        old_model = self.model.find(link=link)
        if old_model is not None:
            logger.info(f"Video already exists in the database: {link}")
            return

        logger.info(f"Starting scraping YouTube video: {link}")

        video_id = self._extract_video_id(link)
        if not video_id:
            logger.warning(f"Could not extract video ID from URL: {link}")
            return

        transcript, language = self._get_transcript(video_id)
        if transcript is None:
            logger.warning(f"No transcript available for video: {link}")
            return

        content = {
            "transcript": transcript,
            "language": language,
        }

        user = kwargs["user"]
        instance = self.model(
            content=content,
            link=link,
            platform="youtube",
            author_id=user.id,
            author_full_name=user.full_name,
        )
        instance.save()

        logger.info(f"Finished scraping YouTube video: {link}")

    def _extract_video_id(self, link: str) -> str | None:
        parsed = urlparse(link)

        if parsed.netloc in ("youtu.be",):
            return parsed.path.lstrip("/").split("?")[0] or None

        if parsed.netloc in ("www.youtube.com", "youtube.com"):
            video_id = parse_qs(parsed.query).get("v", [None])[0]
            return video_id

        return None

    def _get_transcript(self, video_id: str) -> tuple[str | None, str | None]:
        try:
            transcript_list = self._api.list(video_id)

            try:
                transcript = transcript_list.find_transcript(self._languages)
            except NoTranscriptFound:
                transcript = transcript_list.find_generated_transcript(self._languages)

            fetched = transcript.fetch()
            formatter = TextFormatter()
            text = formatter.format_transcript(fetched)
            return text, transcript.language_code

        except TranscriptsDisabled:
            logger.warning(f"Transcripts are disabled for video: {video_id}")
            return None, None
        except NoTranscriptFound:
            logger.warning(f"No transcript found in languages {self._languages} for video: {video_id}")
            return None, None
        except Exception as e:
            logger.error(f"Error fetching transcript for video {video_id}: {e!s}")
            return None, None

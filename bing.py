#!/usr/bin/env python3
import argparse
import json
import logging
import re
import signal
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import List, Set, Dict, Any, Optional
from urllib.parse import urljoin, parse_qs, urlparse, urlencode
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


class ImageDownloader:
    CHUNK_SIZE = 8192
    BING_API_BASE = 'https://global.bing.com/HPImageArchive.aspx'
    BING_API_PARAMS = (
        '?format=js&idx=0&n={days}&pid=hp&FORM=BEHPTB'
        '&uhd=1&uhdwidth=3840&uhdheight=2160'
        '&setmkt={region}&setlang=en'
    )
    API_REGION = 'en-US'
    API_DAYS = 8
    QS_THUMB = 'pid=hp&w=384&h=216&rs=1&c=4'
    QS_THUMB_FEATURED = 'w=1000'
    HTTP_UA = (
        'Mozilla/5.0 (X11; Linux x86_64; rv:145.0) '
        'Gecko/20100101 Firefox/145.0'
    )
    HTTP_HEADERS = {'User-Agent': HTTP_UA}

    def __init__(self, shutdown_event: threading.Event, max_workers: int = 4):
        self.shutdown_event = shutdown_event
        self.max_workers = max_workers
        self.logger = logging.getLogger(self.__class__.__name__)
        self.seen_urls: Set[str] = set()
        self.lock = threading.Lock()
        self.session = requests.Session()
        self.session.headers.update(self.HTTP_HEADERS)

    @staticmethod
    def _normalize_url(url: str) -> str:
        url = url.strip()
        pattern = r'://(?:cn|www)\.bing\.com'
        return re.sub(pattern, '://global.bing.com', url, flags=re.IGNORECASE)

    @staticmethod
    def _build_download_url(url: str) -> str:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        id_list = query.get('id')
        if id_list:
            base = f'{parsed.scheme}://{parsed.netloc}{parsed.path}'
            return f'{base}?id={id_list[0]}'
        return ImageDownloader._normalize_url(url)

    @staticmethod
    def _parse_bing_date(date_str: str) -> Optional[str]:
        if not date_str:
            return None
        date_str = date_str.strip()

        if len(date_str) == 12 and date_str.isdigit():
            try:
                dt = datetime.strptime(date_str[:8], '%Y%m%d')
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                pass

        if len(date_str) == 8 and date_str.isdigit():
            try:
                dt = datetime.strptime(date_str, '%Y%m%d')
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                pass

        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str

        formats = ['%b %d, %Y', '%b %d %Y', '%B %d, %Y', '%B %d %Y']
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
        return None

    @staticmethod
    def _extract_date_string(
        url: str,
        api_item: Optional[Dict[str, Any]] = None
    ) -> str:
        if api_item:
            if api_item.get('date'):
                parsed = ImageDownloader._parse_bing_date(api_item['date'])
                if parsed:
                    return parsed
            if api_item.get('fullstartdate'):
                parsed = ImageDownloader._parse_bing_date(
                    api_item['fullstartdate']
                )
                if parsed:
                    return parsed

        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        id_val_list = query.get('id')

        if id_val_list:
            id_val = id_val_list[0]
            match = re.search(r'(\d{8})', id_val)
            if match:
                res = ImageDownloader._parse_bing_date(match.group(1))
                return res or datetime.now().strftime('%Y-%m-%d')

        return datetime.now().strftime('%Y-%m-%d')

    @staticmethod
    def _extract_filename_from_url(url: str, region: str = 'XX') -> str:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        id_val_list = query.get('id')

        if id_val_list:
            id_val = id_val_list[0]
            if id_val.endswith('_UHD.jpg'):
                id_val = id_val[:-len('_UHD.jpg')]

            parts = id_val.split('_', 2)
            if len(parts) >= 2:
                prefix = parts[0]
                lang_region = parts[1]
                if lang_region[:2].isalpha():
                    lang = lang_region[:2].upper()
                else:
                    lang = region.split('-')[0].upper()
                clean = f'{prefix}_{lang}'
            else:
                clean = re.sub(r'[<>:"/\\|?*]', '_', id_val)
        else:
            base = parsed.path.split('/')[-1]
            clean = re.sub(
                r'_[0-9]+x[0-9]+\.jpg$', '', base, flags=re.IGNORECASE
            )
            clean = clean.rsplit('.', 1)[0]
            if '-' in region:
                lang = region.split('-')[0].upper()
            else:
                lang = region.upper()
            clean = f'{clean}_{lang}'

        filename = f'{clean}.jpg'
        filename = re.sub(r'^OHR\.', '', filename, flags=re.IGNORECASE)
        return re.sub(r'[<>:"/\\|?*]', '_', filename)

    def _get_save_path(
        self,
        url: str,
        output_dir_image: Path,
        api_item: Optional[Dict[str, Any]] = None
    ) -> Path:
        date_str = self._extract_date_string(url, api_item)
        try:
            year, month, day = date_str.split('-')
            save_dir = output_dir_image / year / month / day
        except ValueError:
            msg = f'Unexpected date format: {date_str}, using fallback path'
            self.logger.warning(msg)
            save_dir = output_dir_image / 'unknown' / '01' / '01'

        save_dir.mkdir(parents=True, exist_ok=True)
        filename = self._extract_filename_from_url(url)
        return save_dir / filename

    def _download_image(
        self,
        url: str,
        output_dir_image: Path,
        api_item: Optional[Dict[str, Any]] = None
    ) -> bool:
        if self.shutdown_event.is_set():
            return False
        try:
            save_path = self._get_save_path(url, output_dir_image, api_item)
            filename = save_path.name

            if save_path.exists():
                self.logger.info(f'Already exists, skipping: {save_path}')
                return True

            ALLOWED_RANGES = ((2026, 1), (2026, 3))
            now = datetime.now()
            current_ym = (now.year, now.month)
            start, end = ALLOWED_RANGES
            is_allowed = start <= current_ym <= end
            self.logger.debug(
                f'Time check: current={current_ym[0]}-{current_ym[1]:02d}, '
                f'allowed={start[0]}-{start[1]:02d}~{end[0]}-{end[1]:02d}, '
                f'is_allowed={is_allowed}'
            )
            if not is_allowed:
                save_path.touch(exist_ok=True)
                self.logger.info(
                    f'Outside allowed period. Created 0-byte placeholder: {save_path}'
                )
                return True

            download_url = self._build_download_url(url)
            self.logger.info(f'Downloading: {filename}')

            with self.session.get(
                download_url, stream=True, timeout=30
            ) as resp:
                resp.raise_for_status()
                with open(save_path, 'wb') as f:
                    for chunk in resp.iter_content(chunk_size=self.CHUNK_SIZE):
                        if self.shutdown_event.is_set():
                            save_path.unlink(missing_ok=True)
                            self.logger.warning(
                                f'Download interrupted: {filename}'
                            )
                            return False
                        if chunk:
                            f.write(chunk)

            self.logger.info(f'Saved: {save_path}')
            return True
        except Exception as e:
            self.logger.error(f'Failed to download {url}: {e}')
            return False

    def _download_images(
        self,
        download_items: List[Dict[str, Any]],
        output_dir_image: Path,
        total_count: int
    ) -> None:
        if not download_items:
            self.logger.info('No items to download.')
            return

        def download_task(item: Dict[str, Any]) -> bool:
            return self._download_image(
                item['url'],
                output_dir_image,
                api_item=item.get('api_item')
            )

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(download_task, item): item
                for item in download_items
            }
            completed = 0
            success_count = 0

            for future in as_completed(futures):
                if self.shutdown_event.is_set():
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

                completed += 1
                success = future.result()
                item = futures[future]

                if success:
                    success_count += 1
                else:
                    self.logger.warning(f'Download failed: {item["url"]}')

            log_msg = (
                f'Download session finished. '
                f'Processed {completed}/{total_count} items, '
                f'{success_count} successful.'
            )
            self.logger.info(log_msg)

    def _fetch_api_images(self) -> List[Dict[str, Any]]:
        url_template = self.BING_API_BASE + self.BING_API_PARAMS
        url = url_template.format(
            days=self.API_DAYS,
            region=self.API_REGION
        ).strip()

        self.logger.info(f'Fetching from Bing API: {url}')
        try:
            resp = self.session.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            images = []

            for item in data.get('images', []):
                if not item.get('url'):
                    continue
                full_url = urljoin(
                    'https://global.bing.com',
                    item['url'].strip()
                )
                item['url'] = full_url
                images.append(item)

            self.logger.info(f'Fetched {len(images)} wallpapers')
            return images
        except Exception as e:
            self.logger.error(f'API fetch failed: {e}')
            return []

    def _build_thumbnail_url(self, base_url: str, qs: str) -> str:
        base_url = self._normalize_url(base_url)
        parsed = urlparse(base_url)
        existing_params = parse_qs(parsed.query)

        for key in ['w', 'h', 'rs', 'c', 'pid']:
            existing_params.pop(key, None)

        new_params = {
            k: v[0] if isinstance(v, list) else v
            for k, v in existing_params.items()
        }

        thumb_params = {}
        for pair in qs.split('&'):
            if '=' in pair:
                key, value = pair.split('=', 1)
                thumb_params[key] = value

        new_params.update(thumb_params)

        query_str = urlencode(new_params)
        return f'{parsed.scheme}://{parsed.netloc}{parsed.path}?{query_str}'

    def _format_readme_entry(
        self,
        url: str,
        date_str: str,
        title: str = '',
        copyright: str = ''
    ) -> str:
        url = self._normalize_url(url).rstrip()
        thumb_url = self._build_thumbnail_url(url, self.QS_THUMB)
        return f'- ![]({thumb_url}){date_str} [View Image]({url})'

    def _get_date_path(self, api_item: Dict[str, Any]) -> Optional[str]:
        if api_item.get('date'):
            parsed = self._parse_bing_date(api_item['date'])
            if parsed:
                return parsed
        if api_item.get('fullstartdate'):
            parsed = self._parse_bing_date(api_item['fullstartdate'])
            if parsed:
                return parsed
        return None

    def _get_sort_key(self, api_item: Dict[str, Any]) -> str:
        if api_item.get('date'):
            parsed = self._parse_bing_date(api_item['date'])
            if parsed:
                return parsed
        if api_item.get('fullstartdate'):
            parsed = self._parse_bing_date(api_item['fullstartdate'])
            if parsed:
                return parsed
        return ''

    def _get_formatted_date(self, api_item: Dict[str, Any]) -> str:
        if api_item.get('date'):
            parsed = self._parse_bing_date(api_item['date'])
            if parsed:
                return parsed
        if api_item.get('fullstartdate'):
            parsed = self._parse_bing_date(api_item['fullstartdate'])
            if parsed:
                return parsed
        return datetime.now().strftime('%Y-%m-%d')

    def _update_readme(
        self,
        images: List[Dict[str, Any]],
        output_dir_data: Path
    ) -> None:
        if not images:
            return

        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for img in images:
            date_str = self._get_date_path(img)
            if date_str:
                grouped.setdefault(date_str, []).append(img)

        for date_str, day_images in grouped.items():
            day_images.sort(
                key=lambda x: self._get_sort_key(x),
                reverse=True
            )
            try:
                year, month, day = date_str.split('-')
                data_dir = output_dir_data / year / month / day
            except ValueError:
                self.logger.warning(
                    f'Invalid date format for README: {date_str}'
                )
                continue

            data_dir.mkdir(parents=True, exist_ok=True)
            readme_path = data_dir / f'{date_str}.md'

            existing_urls: Set[str] = set()
            if readme_path.exists():
                try:
                    with open(readme_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    pattern = r'\[download 4k\]\((https?://[^\s\)]+)\)'
                    existing_urls = set(re.findall(pattern, content))
                except Exception as e:
                    self.logger.warning(f'Failed to read existing README: {e}')

            new_images = [
                img for img in day_images
                if img['url'] not in existing_urls
            ]
            all_images = day_images

            lines = [f'## Bing Wallpaper ({date_str})', '']
            for img in all_images:
                formatted_date = self._get_formatted_date(img)
                entry = self._format_readme_entry(
                    img['url'],
                    formatted_date,
                    img.get('title', ''),
                    img.get('copyright', '')
                )
                lines.append(entry)

            try:
                with open(readme_path, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(lines) + '\n')

                new_count = len(new_images)
                total_count = len(all_images)
                msg = (
                    f'Updated README: {readme_path} '
                    f'({new_count} new / {total_count} total entries)'
                )
                self.logger.info(msg)
            except Exception as e:
                self.logger.error(f'Failed to write README {readme_path}: {e}')

    def _update_json(
        self,
        images: List[Dict[str, Any]],
        output_dir_data: Path
    ) -> None:
        if not images:
            return

        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for img in images:
            date_str = self._get_date_path(img)
            if date_str:
                grouped.setdefault(date_str, []).append(img)

        for date_str, day_images in grouped.items():
            day_images.sort(
                key=lambda x: self._get_sort_key(x),
                reverse=True
            )
            try:
                year, month, day = date_str.split('-')
                data_dir = output_dir_data / year / month / day
            except ValueError:
                self.logger.warning(
                    f'Invalid date format for JSON: {date_str}'
                )
                continue

            data_dir.mkdir(parents=True, exist_ok=True)
            json_path = data_dir / f'{date_str}.json'

            existing_data: Dict[str, Any] = {'wallpapers': []}
            existing_urls: Set[str] = set()

            if json_path.exists():
                try:
                    with open(json_path, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                    existing_urls = {
                        w.get('url')
                        for w in existing_data.get('wallpapers', [])
                    }
                    count = len(existing_urls)
                    self.logger.debug(
                        f'Loaded {count} existing entries from {json_path}'
                    )
                except Exception as e:
                    self.logger.warning(f'Failed to read existing JSON: {e}')

            new_count = 0
            for img in day_images:
                if img['url'] not in existing_urls:
                    entry = dict(img)
                    parsed_date = self._get_formatted_date(img)
                    entry['date'] = parsed_date
                    entry['download_url'] = self._build_download_url(img['url'])
                    entry['thumbnail'] = self._build_thumbnail_url(
                        img['url'], self.QS_THUMB
                    )
                    entry['featured_thumbnail'] = self._build_thumbnail_url(
                        img['url'], self.QS_THUMB_FEATURED
                    )
                    existing_data['wallpapers'].append(entry)
                    new_count += 1

            existing_data['wallpapers'].sort(
                key=lambda x: self._get_sort_key(x),
                reverse=True
            )
            existing_data['updated_at'] = datetime.now().isoformat()
            existing_data['count'] = len(existing_data['wallpapers'])

            try:
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(
                        existing_data,
                        f,
                        indent=2,
                        ensure_ascii=False
                    )

                total = existing_data["count"]
                msg = (
                    f'Updated JSON: {json_path} '
                    f'({new_count} new / {total} total entries)'
                )
                self.logger.info(msg)
            except Exception as e:
                self.logger.error(f'Failed to write JSON {json_path}: {e}')

    def run_api(
        self,
        output_dir_data: Path,
        output_dir_image: Path
    ) -> None:
        output_dir_image.mkdir(parents=True, exist_ok=True)
        output_dir_data.mkdir(parents=True, exist_ok=True)

        images = self._fetch_api_images()
        if not images:
            self.logger.warning('No images to download.')
            return

        download_items = [
            {'url': img['url'], 'api_item': img}
            for img in images
        ]

        self._download_images(
            download_items,
            output_dir_image,
            len(images)
        )
        self._update_readme(images, output_dir_data)
        self._update_json(images, output_dir_data)

    def cleanup(self) -> None:
        if hasattr(self, 'session'):
            self.session.close()


class App:
    OUTPUT_DIR_IMAGE_DEFAULT = 'image'
    OUTPUT_DIR_DATA_DEFAULT = 'data'
    WORKERS_DEFAULT = 4
    API_REGION_DEFAULT = 'en-US'
    API_DAYS_DEFAULT = 8

    def __init__(self) -> None:
        self.shutdown_event: threading.Event = threading.Event()
        name = self.__class__.__name__
        self.logger: logging.Logger = logging.getLogger(__name__).getChild(name)
        self.args: Optional[argparse.Namespace] = None
        self.downloader: Optional[ImageDownloader] = None

    def _setup_signal_handlers(self) -> None:
        def signal_handler(signum: int, frame) -> None:
            msg = f'Signal {signum} received, initiating shutdown...'
            self.logger.debug(msg)
            self.shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def _setup_logging(self, verbose: bool = False) -> None:
        root_logger = logging.getLogger()
        if not root_logger.handlers:
            handler = logging.StreamHandler(sys.stderr)
            fmt_str = (
                '[%(asctime)s] %(levelname)s '
                '[%(name)s:%(lineno)d] [%(threadName)s] %(message)s'
            )
            formatter = logging.Formatter(
                fmt_str,
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            root_logger.addHandler(handler)
            level = logging.DEBUG if verbose else logging.INFO
            root_logger.setLevel(level)

    def _parse_arguments(self) -> argparse.Namespace:
        desc = (
            'Bing Wallpaper Downloader (API Mode): '
            'Download wallpapers from Bing API'
        )
        parser = argparse.ArgumentParser(description=desc)

        parser.add_argument(
            '-o', '--output',
            type=Path,
            default=Path(self.OUTPUT_DIR_IMAGE_DEFAULT),
            help=f'Output directory for images (default: {self.OUTPUT_DIR_IMAGE_DEFAULT})'
        )
        parser.add_argument(
            '-d', '--data',
            type=Path,
            default=Path(self.OUTPUT_DIR_DATA_DEFAULT),
            help=f'Data directory for JSON/MD (default: {self.OUTPUT_DIR_DATA_DEFAULT})'
        )
        parser.add_argument(
            '-w', '--workers',
            type=int,
            default=self.WORKERS_DEFAULT,
            help=f'Max download threads (default: {self.WORKERS_DEFAULT})'
        )
        parser.add_argument(
            '-v', '--verbose',
            action='store_true',
            help='Enable verbose/debug logging'
        )
        return parser.parse_args()

    def _execute(self) -> None:
        self.downloader = ImageDownloader(
            shutdown_event=self.shutdown_event,
            max_workers=self.args.workers
        )

        output_dir_image = self.args.output
        output_dir_data = self.args.data

        output_dir_image.mkdir(parents=True, exist_ok=True)
        output_dir_data.mkdir(parents=True, exist_ok=True)

        region = ImageDownloader.API_REGION
        days = ImageDownloader.API_DAYS
        self.logger.info(
            f'Using Bing API mode (region: {region}, days: {days})'
        )
        self.logger.info(f'Image output directory: {output_dir_image}')
        self.logger.info(f'Data directory: {output_dir_data}')

        self.downloader.run_api(
            output_dir_data=output_dir_data,
            output_dir_image=output_dir_image
        )

    def on_start(self) -> None:
        pass

    def on_stop(self, exit_code: int) -> None:
        if self.downloader:
            self.downloader.cleanup()

    def run(self) -> int:
        self._setup_signal_handlers()
        self.args = self._parse_arguments()
        self._setup_logging(verbose=self.args.verbose)

        exit_code: int = 0
        try:
            self.on_start()
            self._execute()

            if self.shutdown_event.is_set():
                self.logger.info('Shutdown completed gracefully.')
                exit_code = 0
            else:
                self.logger.info('All done.')
                exit_code = 0
        except SystemExit:
            raise
        except KeyboardInterrupt:
            self.logger.info('Interrupted by user.')
            exit_code = 1
        except OSError:
            if not self.shutdown_event.is_set():
                self.logger.exception('OS-level error occurred')
                exit_code = 1
        except ValueError:
            raise
        except Exception:
            if not self.shutdown_event.is_set():
                self.logger.exception('Unexpected error during execution')
                exit_code = 1
        finally:
            self.on_stop(exit_code)
        return exit_code


if __name__ == '__main__':
    sys.exit(App().run())

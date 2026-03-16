#!/usr/bin/env python3
import argparse
import logging
import shutil
import signal
import sys
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional


class ImageArchiver:
    def __init__(self, shutdown_event: threading.Event) -> None:
        name = self.__class__.__name__
        self.logger: logging.Logger = logging.getLogger(__name__).getChild(name)
        self.shutdown_event = shutdown_event

    def _get_retention_months(self) -> set[tuple[int, int]]:
        now = datetime.now()
        current = (now.year, now.month)

        first_day_of_month = now.replace(day=1)
        last_day_of_prev_month = first_day_of_month - timedelta(days=1)
        prev = (last_day_of_prev_month.year, last_day_of_prev_month.month)

        return {current, prev}

    def _parse_month_dir(self, dir_name: str) -> Optional[tuple[int, int]]:
        parts = dir_name.split('/')
        if len(parts) == 2:
            try:
                year, month = int(parts[0]), int(parts[1])
                if 1 <= month <= 12:
                    return (year, month)
            except ValueError:
                pass
        return None

    def run(self, dir_source: Path, dir_archive: Path) -> int:
        if not dir_source.exists():
            self.logger.error(f'Source directory does not exist: {dir_source}')
            return 1

        if not dir_source.is_dir():
            self.logger.error(f'Source path is not a directory: {dir_source}')
            return 1

        retention = self._get_retention_months()
        self.logger.info(f'Retention policy: keep {sorted(retention)}')

        dirs_to_process: list[Path] = []

        for year_entry in dir_source.iterdir():
            if not year_entry.is_dir():
                continue

            for month_entry in year_entry.iterdir():
                if not month_entry.is_dir():
                    continue

                rel_path = month_entry.relative_to(dir_source)
                month_key = self._parse_month_dir(str(rel_path))

                if month_key is not None:
                    dirs_to_process.append(month_entry)

        dirs_to_process.sort(key=lambda p: str(p.relative_to(dir_source)))

        moved_count = 0
        for entry in dirs_to_process:
            if self.shutdown_event.is_set():
                break

            rel_path = entry.relative_to(dir_source)
            month_key = self._parse_month_dir(str(rel_path))

            if month_key is None:
                continue

            if month_key in retention:
                self.logger.debug(f'Keeping: {entry}')
                continue

            archive_path = dir_archive / rel_path

            if archive_path.exists():
                self.logger.warning(f'Overwriting existing archive: {archive_path}')
                shutil.rmtree(archive_path)

            try:
                archive_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(entry), str(archive_path))
                self.logger.info(f'Moved: {entry} → {archive_path}')
                moved_count += 1
            except Exception as e:
                self.logger.error(f'Failed to move {entry}: {e}')

        if moved_count == 0:
            self.logger.info('No directories required archiving.')
        elif moved_count == 1:
            self.logger.info('Archiving complete. Moved 1 directory.')
        else:
            self.logger.info(f'Archiving complete. Moved {moved_count} directories.')

        return 0


class App:
    DIR_SOURCE = 'image'
    DIR_ARCHIVE = 'image-archive'

    def __init__(self) -> None:
        name = self.__class__.__name__
        self.logger: logging.Logger = logging.getLogger(__name__).getChild(name)
        self.shutdown_event: threading.Event = threading.Event()
        self.args: Optional[argparse.Namespace] = None
        self.archiver: Optional[ImageArchiver] = None

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
        desc = 'Prune wallpaper directories to the last 2 months.'
        parser = argparse.ArgumentParser(description=desc)

        parser.add_argument(
            '-s', '--source',
            type=Path,
            default=Path(self.DIR_SOURCE),
            help=f'Source directory (default: {self.DIR_SOURCE})'
        )
        parser.add_argument(
            '-a', '--archive',
            type=Path,
            default=Path(self.DIR_ARCHIVE),
            help=f'Archive directory (default: {self.DIR_ARCHIVE})'
        )
        parser.add_argument(
            '-v', '--verbose',
            action='store_true',
            help='Enable verbose/debug logging'
        )
        return parser.parse_args()

    def _execute(self) -> None:
        dir_source = self.args.source
        dir_archive = self.args.archive

        if not dir_source.exists():
            self.logger.error(f'Source directory does not exist: {dir_source}')
            return

        if not dir_source.is_dir():
            self.logger.error(f'Source path is not a directory: {dir_source}')
            return

        dir_archive.mkdir(parents=True, exist_ok=True)

        self.logger.info(f'Source directory: {dir_source}')
        self.logger.info(f'Archive directory: {dir_archive}')

        self.archiver = ImageArchiver(
            shutdown_event=self.shutdown_event
        )
        self.archiver.run(
            dir_source=dir_source,
            dir_archive=dir_archive
        )

    def on_start(self) -> None:
        pass

    def on_stop(self, exit_code: int) -> None:
        pass

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

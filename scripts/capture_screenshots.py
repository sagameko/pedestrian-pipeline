"""Regenerate the dashboard screenshots used in the README.

Starts the Streamlit app against the local warehouse, captures a full-page shot
plus one crop per section, downsamples from 2x, and writes them to docs/images.

    uv run --extra dashboard --extra screenshots python scripts/capture_screenshots.py

Screenshots are a build artifact, not a hand-taken one-off: regenerating them
after a UI change is a single command, so they never drift from the app.
"""

import argparse
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from PIL import Image
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent.parent
APP = ROOT / "src" / "pedestrian_pipeline" / "dashboard.py"
PORT = 8501
URL = f"http://localhost:{PORT}"

# Capture at 2x then halve, so text stays crisp without shipping a huge file.
CAPTURE_WIDTH = 1440
SCALE = 2

# Streamlit's own chrome is framework furniture, not product.
HIDE_CHROME = """
header[data-testid="stHeader"], [data-testid="stToolbar"],
[data-testid="stDecoration"], [data-testid="stStatusWidget"],
#MainMenu, footer { display: none !important; }
"""

GEOMETRY = """() => {
  const main = document.querySelector('[data-testid="stMain"]');
  const box = main.getBoundingClientRect();
  const heads = {};
  for (const h of document.querySelectorAll('h3')) {
    heads[h.textContent.trim()] = h.getBoundingClientRect().top + window.scrollY;
  }
  let bottom = 0;
  for (const el of main.querySelectorAll('[data-testid="stVerticalBlock"] > div')) {
    const r = el.getBoundingClientRect();
    if (r.height > 0) bottom = Math.max(bottom, r.bottom + window.scrollY);
  }
  return {x: box.x, width: box.width, contentBottom: bottom || main.scrollHeight, heads};
}"""


def wait_for_server(timeout: int = 60) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(URL, timeout=2) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, OSError):
            time.sleep(1)
    raise RuntimeError(f"Streamlit did not come up on {URL} within {timeout}s")


def capture(out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            channel="chrome",
            # The map is a WebGL layer; headless needs a software rasteriser.
            args=["--enable-unsafe-swiftshader", "--use-gl=angle", "--use-angle=swiftshader"],
        )
        page = browser.new_page(
            viewport={"width": CAPTURE_WIDTH, "height": 1000}, device_scale_factor=SCALE
        )
        page.goto(URL, wait_until="domcontentloaded", timeout=90_000)
        page.wait_for_selector('[data-testid="stMetricValue"]', timeout=60_000)
        page.add_style_tag(content=HIDE_CHROME)
        page.wait_for_timeout(5000)

        # Streamlit scrolls inside section[data-testid="stMain"], so the document
        # never grows and full_page would only capture the viewport. Measure the
        # real height and make the viewport tall enough to hold the whole app.
        height = page.evaluate("document.querySelector('[data-testid=\"stMain\"]').scrollHeight")
        page.set_viewport_size({"width": CAPTURE_WIDTH, "height": int(height) + 80})
        page.wait_for_timeout(7000)

        overview = out_dir / "dashboard-overview.png"
        page.screenshot(path=overview)
        written.append(overview)

        geo = page.evaluate(GEOMETRY)
        heads, bottom = geo["heads"], geo["contentBottom"] + 24
        sections = {
            "dashboard-rhythm": (0, heads["Busiest locations"] - 28),
            "dashboard-rankings": (
                heads["Busiest locations"] - 28,
                heads["Where the sensors are"] - 40,
            ),
            "dashboard-quality": (heads["Where the sensors are"] - 28, bottom),
        }

        for name, (top, end) in sections.items():
            top = max(top, 0)
            target = out_dir / f"{name}.png"
            page.screenshot(
                path=target,
                clip={
                    "x": geo["x"],
                    "y": top,
                    "width": geo["width"],
                    "height": min(end, bottom) - top,
                },
            )
            written.append(target)

        browser.close()

    return written


def optimise(paths: list[Path]) -> None:
    for path in paths:
        before = path.stat().st_size / 1024
        image = Image.open(path).convert("RGB")
        image = image.resize((image.width // SCALE, image.height // SCALE), Image.LANCZOS)
        image.quantize(colors=256, dither=Image.FLOYDSTEINBERG).save(path, optimize=True)
        print(f"  {path.name:28} {before:6.0f} KB -> {path.stat().st_size / 1024:5.0f} KB")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=ROOT / "docs" / "images")
    parser.add_argument("--database", type=Path, default=ROOT / "data" / "pedestrian.duckdb")
    args = parser.parse_args()

    if not args.database.exists():
        print(f"no warehouse at {args.database}; run `uv run ingest` first", file=sys.stderr)
        return 1

    server = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            str(APP),
            "--server.port",
            str(PORT),
            "--server.headless",
            "true",
            "--server.fileWatcherType",
            "none",
        ],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )
    try:
        wait_for_server()
        print(f"capturing from {URL}")
        written = capture(args.out)
        print("optimising")
        optimise(written)
    finally:
        server.terminate()
        server.wait(timeout=10)

    print(f"wrote {len(written)} screenshots to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

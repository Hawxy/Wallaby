"""Regenerate docs/public/og.png (the social card) and apple-touch-icon.png.

Run after changing the tagline, the sink list, or the brand gradient in
docs/.vitepress/theme/custom.css. Not part of CI: it needs Pillow and the
licensed Berkeley Mono woff2, which is copied in from a private repo on deploy.

    py -m venv .venv && .venv/Scripts/pip install Pillow
    .venv/Scripts/python assets/make-og-image.py
"""
import math
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parent.parent
FONT = ROOT / 'docs/.vitepress/theme/fonts/Berkeley Mono Variable.woff2'
ICON = ROOT / 'assets/icon-512.png'
OG = ROOT / 'docs/public/og.png'
TOUCH_ICON = ROOT / 'docs/public/apple-touch-icon.png'

S = 2                      # supersample factor, downsampled at the end
W, H = 1200 * S, 630 * S

# Kept in sync with the dark theme in docs/.vitepress/theme/custom.css.
BG = (13, 13, 13)          # --vp-c-bg
TEXT_1 = (224, 224, 224)   # --vp-c-text-1
TEXT_3 = (125, 125, 125)   # --vp-c-text-3
AMBER = (255, 180, 84)     # #ffb454, hero gradient start
BLUE = (89, 194, 255)      # #59c2ff, hero gradient end
HERO_ANGLE = 150           # --vp-home-hero-name-background
AMBER_STOP = 0.30          # the "30%" stop on that gradient

TAGLINE = ['Postgres CDC Engine', 'for .NET']
SINKS = ['Meilisearch  Elasticsearch  OpenSearch',
         'Kafka  pgvector  HTTP  custom sinks']


def font(size, name='Regular'):
    f = ImageFont.truetype(str(FONT), size * S)
    f.set_variation_by_name(name)
    return f


def lerp(t):
    return tuple(round(a + (b - a) * t) for a, b in zip(AMBER, BLUE))


def gradient_text(img, xy, text, fnt, angle_deg=HERO_ANGLE):
    """Fill text with the hero gradient, matching the CSS angle and colour stop."""
    mask = Image.new('L', img.size, 0)
    ImageDraw.Draw(mask).text(xy, text, font=fnt, fill=255)
    bbox = mask.getbbox()
    if not bbox:
        return
    x0, y0, x1, y1 = bbox

    # CSS angles run clockwise from north, so the direction vector in screen
    # space (y growing downwards) is (sin, -cos).
    th = math.radians(angle_deg)
    dx, dy = math.sin(th), -math.cos(th)
    projections = [x * dx + y * dy for x in (x0, x1) for y in (y0, y1)]
    lo, hi = min(projections), max(projections)
    span = (hi - lo) or 1

    grad = Image.new('RGB', (x1 - x0, y1 - y0))
    px = grad.load()
    for y in range(y1 - y0):
        for x in range(x1 - x0):
            t = (((x + x0) * dx + (y + y0) * dy) - lo) / span
            t = 0.0 if t <= AMBER_STOP else (t - AMBER_STOP) / (1 - AMBER_STOP)
            px[x, y] = lerp(min(max(t, 0.0), 1.0))
    img.paste(grad, (x0, y0), mask.crop(bbox))


def build_card():
    img = Image.new('RGB', (W, H), BG)
    d = ImageDraw.Draw(img)

    bar = 5 * S
    for x in range(W):
        d.line([(x, 0), (x, bar)], fill=lerp(x / W))

    # The source icon sits on solid black, so its own luminance becomes the
    # alpha mask. Pasting it directly would composite as a visible square.
    size = 470 * S
    icon = Image.open(ICON).convert('RGB').resize((size, size), Image.LANCZOS)
    icon.putalpha(icon.convert('L').point(lambda v: min(255, int(v * 1.35))))
    img.paste(icon, (W - int(size * 0.72), (H - size) // 2), icon)

    x = 84 * S
    gradient_text(img, (x, 150 * S), 'Wallaby', font(96, 'Bold'))
    for i, line in enumerate(TAGLINE):
        d.text((x, (286 + i * 54) * S), line, font=font(44, 'Medium'), fill=TEXT_1)

    d.line([(x, 428 * S), (x + 300 * S, 428 * S)], fill=(45, 45, 45), width=S)

    for i, line in enumerate(SINKS):
        d.text((x, (458 + i * 32) * S), line, font=font(21, 'Regular'), fill=TEXT_3)

    d.text((x, 552 * S), 'wallabycdc.net', font=font(20, 'SemiBold'), fill=AMBER)

    img.resize((1200, 630), Image.LANCZOS).save(OG, 'PNG', optimize=True)
    print(f'wrote {OG.relative_to(ROOT)} ({OG.stat().st_size // 1024} KB)')


def build_touch_icon():
    # Apple composites onto an opaque tile, so keep the icon's solid ground.
    src = Image.open(ICON).convert('RGB')
    src.resize((180, 180), Image.LANCZOS).save(TOUCH_ICON, 'PNG', optimize=True)
    print(f'wrote {TOUCH_ICON.relative_to(ROOT)} ({TOUCH_ICON.stat().st_size // 1024} KB)')


if __name__ == '__main__':
    build_card()
    build_touch_icon()

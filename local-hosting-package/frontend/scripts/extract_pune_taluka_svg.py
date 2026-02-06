"""
Extract Pune taluka (14-region) outlines from the provided pune-map.png as SVG paths.

This is image-vectorization (not geographic). It is used to achieve pixel-perfect
alignment with the reference PNG while still having clickable regions.

Implementation notes:
- Seeds in the source image may fall on background/text; we auto-search for a
  nearby representative pixel within a bounding box for each taluka.
- Contours are produced by converting the filled region mask into boundary
  segments and chaining them into a polygon loop (pixel-exact).

Output:
  - frontend/src/components/puneTalukaPaths.json
"""

from __future__ import annotations

import json
from collections import defaultdict, deque
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import numpy as np
from PIL import Image


ROOT = Path(__file__).resolve().parents[1]
# Source image lives under frontend/public so CRA can serve it as a static asset too.
IMG_PATH = ROOT / "public" / "pune-map.png"
OUT_PATH = ROOT / "src" / "components" / "puneTalukaPaths.json"


# Approx bounding boxes per taluka in original 720x540 image coordinates.
# These are used to auto-pick a good seed pixel inside each region.
BOXES = {
    "JUNNAR": (220, 80, 520, 210),
    "AMBEGAON": (250, 150, 450, 270),
    "KHED": (180, 180, 380, 310),
    "MAVAL": (90, 160, 250, 300),
    "MULSHI": (90, 250, 260, 380),
    "VELHE": (80, 330, 240, 460),
    "BHOR": (190, 360, 360, 520),
    "PURANDHAR": (280, 300, 460, 470),
    "PUNE CITY": (230, 250, 330, 340),
    "HAVELI": (300, 250, 420, 350),
    "SHIRUR": (360, 170, 560, 320),
    "DAUND": (360, 260, 560, 400),
    "BARAMATI": (360, 360, 540, 520),
    "INDAPUR": (470, 360, 680, 520),
}


def _rgb_dist(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    # Euclidean distance in RGB
    d = a.astype(np.int16) - b.astype(np.int16)
    return np.sqrt((d * d).sum(axis=-1))


def _flood_region(img: np.ndarray, seed_xy: Tuple[int, int], tol: float = 45.0) -> np.ndarray:
    """Flood fill from seed for pixels within tol of the seed color."""
    h, w, _ = img.shape
    sx, sy = seed_xy
    sx = int(np.clip(sx, 0, w - 1))
    sy = int(np.clip(sy, 0, h - 1))
    seed = img[sy, sx].copy()

    visited = np.zeros((h, w), dtype=np.uint8)
    mask = np.zeros((h, w), dtype=np.uint8)
    q = deque([(sx, sy)])
    visited[sy, sx] = 1

    # Precompute distance threshold by comparing on the fly (cheap enough at this size)
    while q:
        x, y = q.popleft()
        px = img[y, x]
        if np.linalg.norm(px.astype(np.int16) - seed.astype(np.int16)) <= tol:
            mask[y, x] = 1
            for nx, ny in ((x - 1, y), (x + 1, y), (x, y - 1), (x, y + 1)):
                if 0 <= nx < w and 0 <= ny < h and not visited[ny, nx]:
                    visited[ny, nx] = 1
                    q.append((nx, ny))
    return mask.astype(bool)


def _pick_seed(img: np.ndarray, box: Tuple[int, int, int, int], step: int = 3) -> Optional[Tuple[int, int]]:
    """
    Pick a seed pixel inside a colored region within the given box.
    We avoid near-white background and choose a pixel that yields a large flood region.
    """
    h, w, _ = img.shape
    x0, y0, x1, y1 = box
    x0, y0 = max(0, x0), max(0, y0)
    x1, y1 = min(w - 1, x1), min(h - 1, y1)

    bg = np.array([242, 242, 242], dtype=np.int16)

    best = None
    best_area = -1

    # Coarse scan
    for y in range(y0, y1, step):
        for x in range(x0, x1, step):
            px = img[y, x].astype(np.int16)
            if np.linalg.norm(px - bg) < 18:
                continue
            # Skip near-white (borders/labels)
            if px.min() > 235:
                continue
            region = _flood_region(img, (x, y), tol=45.0)
            area = int(region.sum())
            # Reject absurdly large (leaked background)
            if area > (w * h * 0.6):
                continue
            if area > best_area:
                best_area = area
                best = (x, y)

    return best


def _mask_to_segments(mask: np.ndarray) -> List[Tuple[Tuple[int, int], Tuple[int, int]]]:
    """
    Convert a boolean mask into boundary segments along pixel edges.
    Segments are between integer grid points in (x,y) where x in [0..w], y in [0..h].
    """
    h, w = mask.shape
    segs: List[Tuple[Tuple[int, int], Tuple[int, int]]] = []
    for y in range(h):
        for x in range(w):
            if not mask[y, x]:
                continue
            # left edge
            if x == 0 or not mask[y, x - 1]:
                segs.append(((x, y), (x, y + 1)))
            # right edge
            if x == w - 1 or not mask[y, x + 1]:
                segs.append(((x + 1, y), (x + 1, y + 1)))
            # top edge
            if y == 0 or not mask[y - 1, x]:
                segs.append(((x, y), (x + 1, y)))
            # bottom edge
            if y == h - 1 or not mask[y + 1, x]:
                segs.append(((x, y + 1), (x + 1, y + 1)))
    return segs


def _segments_to_loop(segs: List[Tuple[Tuple[int, int], Tuple[int, int]]]) -> List[Tuple[int, int]]:
    """
    Chain boundary segments into a single loop.
    If multiple loops exist, returns the longest one.
    """
    if not segs:
        return []

    adj: Dict[Tuple[int, int], List[Tuple[int, int]]] = defaultdict(list)
    for a, b in segs:
        adj[a].append(b)
        adj[b].append(a)

    visited_edges = set()
    loops: List[List[Tuple[int, int]]] = []

    def edge_key(u, v):
        return (u, v) if u <= v else (v, u)

    for start in list(adj.keys()):
        # find an unused edge from start
        for nxt in adj[start]:
            ek = edge_key(start, nxt)
            if ek in visited_edges:
                continue
            # walk
            loop = [start]
            prev = start
            cur = nxt
            visited_edges.add(ek)
            for _ in range(200000):
                loop.append(cur)
                nbrs = adj[cur]
                # choose the neighbor that's not prev, preferring unused edges
                candidates = [p for p in nbrs if p != prev]
                if not candidates:
                    break
                # pick a candidate with an unused edge if possible
                chosen = None
                for cand in candidates:
                    if edge_key(cur, cand) not in visited_edges:
                        chosen = cand
                        break
                if chosen is None:
                    chosen = candidates[0]
                ek2 = edge_key(cur, chosen)
                if ek2 not in visited_edges:
                    visited_edges.add(ek2)
                prev, cur = cur, chosen
                if cur == start:
                    # Close the loop explicitly (otherwise loop[-1] won't be start)
                    loop.append(cur)
                    break
            if len(loop) > 10 and loop[-1] == start:
                loops.append(loop)

    if not loops:
        return []
    loops.sort(key=len, reverse=True)
    return loops[0]


def _boundary_pixels(mask: np.ndarray) -> np.ndarray:
    """Return boundary pixels of a boolean mask."""
    h, w = mask.shape
    # A pixel is boundary if it's True and at least one 4-neighbor is False/outside.
    b = np.zeros_like(mask, dtype=bool)
    for y in range(h):
        for x in range(w):
            if not mask[y, x]:
                continue
            if x == 0 or x == w - 1 or y == 0 or y == h - 1:
                b[y, x] = True
                continue
            if not (mask[y, x - 1] and mask[y, x + 1] and mask[y - 1, x] and mask[y + 1, x]):
                b[y, x] = True
    return b


def _trace_contour(boundary: np.ndarray) -> List[Tuple[int, int]]:
    """
    Trace a single contour from boundary pixels using Moore-neighbor tracing.
    Returns list of (x,y) points.
    """
    h, w = boundary.shape
    ys, xs = np.where(boundary)
    if len(xs) == 0:
        return []

    # Start at top-most, left-most boundary pixel
    start_idx = np.lexsort((xs, ys))[0]
    sx, sy = int(xs[start_idx]), int(ys[start_idx])

    # 8-neighborhood directions clockwise
    dirs = [(-1, 0), (-1, -1), (0, -1), (1, -1), (1, 0), (1, 1), (0, 1), (-1, 1)]
    dir_idx = 0
    x, y = sx, sy
    contour = [(x, y)]
    visited = set([(x, y, dir_idx)])

    # Follow until we return to start and have made progress
    for _ in range(w * h * 2):
        found = False
        # Search neighbors starting from dir_idx (Moore tracing)
        for i in range(8):
            dx, dy = dirs[(dir_idx + i) % 8]
            nx, ny = x + dx, y + dy
            if 0 <= nx < w and 0 <= ny < h and boundary[ny, nx]:
                x, y = nx, ny
                dir_idx = (dir_idx + i + 5) % 8  # heuristic to keep hugging boundary
                contour.append((x, y))
                key = (x, y, dir_idx)
                if key in visited and (x, y) == (sx, sy) and len(contour) > 50:
                    return contour
                visited.add(key)
                found = True
                break
        if not found:
            break
    return contour


def _rdp(points: List[Tuple[float, float]], eps: float) -> List[Tuple[float, float]]:
    """Ramer–Douglas–Peucker polyline simplification."""
    if len(points) < 3:
        return points

    x1, y1 = points[0]
    x2, y2 = points[-1]
    dx, dy = x2 - x1, y2 - y1
    denom = (dx * dx + dy * dy) ** 0.5 or 1.0

    max_dist = -1.0
    idx = -1
    for i in range(1, len(points) - 1):
        x0, y0 = points[i]
        # distance point to line
        dist = abs(dy * x0 - dx * y0 + x2 * y1 - y2 * x1) / denom
        if dist > max_dist:
            max_dist = dist
            idx = i

    if max_dist > eps:
        left = _rdp(points[: idx + 1], eps)
        right = _rdp(points[idx:], eps)
        return left[:-1] + right
    return [points[0], points[-1]]


def _simplify_ring(points: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    """
    Simplify a closed polygon ring by removing consecutive duplicates and collinear points.
    Works well for pixel-edge contours without needing external deps.
    """
    if not points:
        return []

    ring = points
    if len(ring) > 1 and ring[0] == ring[-1]:
        ring = ring[:-1]

    if len(ring) < 3:
        return ring

    # Remove consecutive duplicates
    dedup: List[Tuple[int, int]] = []
    for p in ring:
        if not dedup or p != dedup[-1]:
            dedup.append(p)

    if len(dedup) < 3:
        return dedup

    # Remove collinear points (cross product == 0)
    out: List[Tuple[int, int]] = []
    n = len(dedup)
    for i in range(n):
        prev = dedup[(i - 1) % n]
        cur = dedup[i]
        nxt = dedup[(i + 1) % n]
        vx1, vy1 = cur[0] - prev[0], cur[1] - prev[1]
        vx2, vy2 = nxt[0] - cur[0], nxt[1] - cur[1]
        cross = vx1 * vy2 - vy1 * vx2
        if cross != 0:
            out.append(cur)

    return out if len(out) >= 3 else dedup


def _to_svg_path(points: List[Tuple[int, int]]) -> str:
    if not points:
        return ""

    # NOTE: points is typically a closed loop (first == last). RDP collapses when endpoints match,
    # so we first simplify the ring in a closure-safe way.
    simp = _simplify_ring(points)
    if not simp:
        return ""

    d = [f"M {float(simp[0][0]):.1f} {float(simp[0][1]):.1f}"]
    for x, y in simp[1:]:
        d.append(f"L {float(x):.1f} {float(y):.1f}")
    d.append("Z")
    return " ".join(d)


def main():
    if not IMG_PATH.exists():
        raise SystemExit(f"Missing image: {IMG_PATH}")

    img = Image.open(IMG_PATH).convert("RGB")
    arr = np.array(img)
    h, w, _ = arr.shape
    assert (w, h) == (720, 540), (w, h)

    paths: Dict[str, Dict[str, object]] = {}
    for name, box in BOXES.items():
        seed = _pick_seed(arr, box)
        if not seed:
            print("WARN: could not pick seed for", name)
            continue
        region = _flood_region(arr, seed, tol=45.0)

        segs = _mask_to_segments(region)
        loop = _segments_to_loop(segs)
        if not loop:
            print("WARN: no loop for", name, "seed", seed, "area", int(region.sum()))
            continue

        d = _to_svg_path(loop)
        if not d:
            print("WARN: no path for", name)
            continue

        paths[name] = {"viewBox": [0, 0, w, h], "path": d, "seed": [int(seed[0]), int(seed[1])]}

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(paths, indent=2), encoding="utf-8")
    print("Wrote", OUT_PATH, "items", len(paths))


if __name__ == "__main__":
    main()



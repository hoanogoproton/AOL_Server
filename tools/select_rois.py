"""Khoanh 3 ROI Body/End/Head va ghi de roi_rules.csv."""

import argparse
import csv
from pathlib import Path

import cv2


ROI_DEFINITIONS = (
    ("roi_body", "BODY", "AOL-Body", "./models/AOL-Body.pt", "Body OK"),
    ("roi_end", "END", "AOL-End", "./models/AOL-End.pt", "End OK"),
    ("roi_head", "HEAD", "AOL-Head", "./models/AOL-Head.pt", "Head OK"),
)

CSV_FIELDS = (
    "inspection_step",
    "roi_id",
    "part_name",
    "model_name",
    "model_path",
    "expected_class",
    "compare_x_min",
    "compare_y_min",
    "compare_x_max",
    "compare_y_max",
    "confidence",
)


def build_rows(rois: list[tuple[int, int, int, int]]) -> list[dict[str, str]]:
    rows = []
    for step in ("STEP_1", "STEP_3"):
        for definition, (x, y, width, height) in zip(ROI_DEFINITIONS, rois):
            roi_id, part_name, model_name, model_path, expected_class = definition
            rows.append(
                {
                    "inspection_step": step,
                    "roi_id": roi_id,
                    "part_name": part_name,
                    "model_name": model_name,
                    "model_path": model_path,
                    "expected_class": expected_class,
                    "compare_x_min": str(x),
                    "compare_y_min": str(y),
                    "compare_x_max": str(x + width),
                    "compare_y_max": str(y + height),
                    "confidence": "0.50",
                }
            )
    return rows


def write_rules(csv_path: Path, rois: list[tuple[int, int, int, int]]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(build_rows(rois))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Chon lan luot ROI Body, End, Head tren anh 1600x1200"
    )
    parser.add_argument("--image", required=True, help="Anh mau 1600x1200")
    parser.add_argument("--csv", default="roi_rules.csv", help="CSV can ghi de")
    args = parser.parse_args()

    image = cv2.imread(args.image)
    if image is None:
        raise SystemExit(f"Khong doc duoc anh: {args.image}")
    height, width = image.shape[:2]
    if (width, height) != (1600, 1200):
        raise SystemExit(
            f"Kich thuoc anh phai la 1600x1200, hien tai {width}x{height}"
        )

    rois = []
    for _, part_name, *_ in ROI_DEFINITIONS:
        window = f"Chon ROI {part_name} - Enter de xac nhan, Esc de huy"
        x, y, roi_width, roi_height = cv2.selectROI(window, image, False, False)
        cv2.destroyWindow(window)
        if roi_width <= 0 or roi_height <= 0:
            raise SystemExit("Da huy: chua chon du 3 ROI")
        rois.append((int(x), int(y), int(roi_width), int(roi_height)))

    write_rules(Path(args.csv), rois)
    print(f"Da ghi 6 classifier ROI rules vao {args.csv}")


if __name__ == "__main__":
    main()

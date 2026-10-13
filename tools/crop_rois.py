"""Cat anh trong thu muc theo cac ROI khai bao trong file CSV."""

import argparse
import csv
from dataclasses import dataclass, field
from pathlib import Path

import cv2


IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png"}
REQUIRED_CSV_FIELDS = {
    "roi_id",
    "part_name",
    "compare_x_min",
    "compare_y_min",
    "compare_x_max",
    "compare_y_max",
}


@dataclass(frozen=True)
class RoiCropRule:
    roi_id: str
    part_name: str
    x_min: int
    y_min: int
    x_max: int
    y_max: int


@dataclass
class CropDirectoryResult:
    processed_images: int = 0
    saved_crops: int = 0
    errors: list[str] = field(default_factory=list)


def load_unique_roi_rules(csv_path: Path) -> list[RoiCropRule]:
    """Doc ROI va bo cac dong trung lap giua cac inspection step."""
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = set(reader.fieldnames or [])
        missing_fields = REQUIRED_CSV_FIELDS - fields
        if missing_fields:
            missing = ", ".join(sorted(missing_fields))
            raise ValueError(f"CSV thieu cot bat buoc: {missing}")

        rules = []
        seen = set()
        for line_number, row in enumerate(reader, start=2):
            try:
                rule = RoiCropRule(
                    roi_id=row["roi_id"].strip(),
                    part_name=row["part_name"].strip(),
                    x_min=int(float(row["compare_x_min"])),
                    y_min=int(float(row["compare_y_min"])),
                    x_max=int(float(row["compare_x_max"])),
                    y_max=int(float(row["compare_y_max"])),
                )
            except (TypeError, ValueError) as error:
                raise ValueError(f"Dong CSV {line_number} co toa do khong hop le") from error

            if not rule.part_name or Path(rule.part_name).name != rule.part_name:
                raise ValueError(f"Dong CSV {line_number} co part_name khong hop le")

            key = (rule.part_name, rule.x_min, rule.y_min, rule.x_max, rule.y_max)
            if key not in seen:
                seen.add(key)
                rules.append(rule)

    if not rules:
        raise ValueError("CSV khong co ROI de cat")
    return rules


def crop_directory(
    input_dir: Path, output_dir: Path, rules: list[RoiCropRule]
) -> CropDirectoryResult:
    """Cat cac anh nam truc tiep trong input_dir va luu theo part_name."""
    result = CropDirectoryResult()
    for image_path in sorted(input_dir.iterdir()):
        if not image_path.is_file() or image_path.suffix.lower() not in IMAGE_SUFFIXES:
            continue

        image = cv2.imread(str(image_path))
        if image is None:
            result.errors.append(f"Khong doc duoc anh: {image_path}")
            continue

        result.processed_images += 1
        height, width = image.shape[:2]
        for rule in rules:
            if not _is_valid_roi(rule, width, height):
                result.errors.append(
                    f"ROI {rule.roi_id} khong nam trong anh {image_path.name} "
                    f"({width}x{height})"
                )
                continue

            destination = output_dir / rule.part_name / image_path.name
            destination.parent.mkdir(parents=True, exist_ok=True)
            crop = image[rule.y_min : rule.y_max, rule.x_min : rule.x_max]
            if not cv2.imwrite(str(destination), crop):
                result.errors.append(f"Khong ghi duoc anh cat: {destination}")
                continue
            result.saved_crops += 1
    return result


def _is_valid_roi(rule: RoiCropRule, width: int, height: int) -> bool:
    return (
        0 <= rule.x_min < rule.x_max <= width
        and 0 <= rule.y_min < rule.y_max <= height
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Cat anh trong thu muc theo roi_rules.csv")
    parser.add_argument("--input-dir", required=True, type=Path, help="Thu muc anh goc")
    parser.add_argument("--csv", default=Path("roi_rules.csv"), type=Path, help="File ROI CSV")
    parser.add_argument("--output-dir", required=True, type=Path, help="Thu muc luu anh cat")
    args = parser.parse_args()

    if not args.input_dir.is_dir():
        raise SystemExit(f"Thu muc anh goc khong ton tai: {args.input_dir}")
    if not args.csv.is_file():
        raise SystemExit(f"Khong tim thay file CSV: {args.csv}")

    try:
        rules = load_unique_roi_rules(args.csv)
        result = crop_directory(args.input_dir, args.output_dir, rules)
    except ValueError as error:
        raise SystemExit(str(error)) from error

    print(f"Da xu ly {result.processed_images} anh, luu {result.saved_crops} anh cat")
    for error in result.errors:
        print(f"LOI: {error}")


if __name__ == "__main__":
    main()

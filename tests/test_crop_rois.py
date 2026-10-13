import csv

import cv2
import numpy as np

from tools.crop_rois import crop_directory, load_unique_roi_rules


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


def write_rules(csv_path, rows):
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def make_row(step, roi_id, part_name, x_min, y_min, x_max, y_max):
    return {
        "inspection_step": step,
        "roi_id": roi_id,
        "part_name": part_name,
        "model_name": "AOL",
        "model_path": "./models/AOL.pt",
        "expected_class": "OK",
        "compare_x_min": str(x_min),
        "compare_y_min": str(y_min),
        "compare_x_max": str(x_max),
        "compare_y_max": str(y_max),
        "confidence": "0.5",
    }


def test_load_unique_roi_rules_ignores_duplicate_steps(tmp_path):
    csv_path = tmp_path / "roi_rules.csv"
    write_rules(
        csv_path,
        [
            make_row("STEP_1", "roi_body", "BODY", 1, 2, 5, 6),
            make_row("STEP_3", "roi_body", "BODY", 1, 2, 5, 6),
            make_row("STEP_1", "roi_end", "END", 6, 2, 9, 6),
        ],
    )

    rules = load_unique_roi_rules(csv_path)

    assert [(rule.part_name, rule.x_min, rule.y_min, rule.x_max, rule.y_max) for rule in rules] == [
        ("BODY", 1, 2, 5, 6),
        ("END", 6, 2, 9, 6),
    ]


def test_crop_directory_saves_each_roi_in_its_part_folder(tmp_path):
    csv_path = tmp_path / "roi_rules.csv"
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    write_rules(
        csv_path,
        [
            make_row("STEP_1", "roi_body", "BODY", 1, 1, 4, 3),
            make_row("STEP_3", "roi_body", "BODY", 1, 1, 4, 3),
            make_row("STEP_1", "roi_end", "END", 4, 0, 6, 2),
            make_row("STEP_1", "roi_head", "HEAD", 0, 2, 2, 4),
        ],
    )
    image = np.zeros((4, 6, 3), dtype=np.uint8)
    image[1:3, 1:4] = (10, 20, 30)
    assert cv2.imwrite(str(input_dir / "tube_01.png"), image)
    (input_dir / "bo_qua.txt").write_text("khong phai anh", encoding="utf-8")

    result = crop_directory(input_dir, output_dir, load_unique_roi_rules(csv_path))

    assert result.processed_images == 1
    assert result.saved_crops == 3
    body = cv2.imread(str(output_dir / "BODY" / "tube_01.png"))
    end = cv2.imread(str(output_dir / "END" / "tube_01.png"))
    head = cv2.imread(str(output_dir / "HEAD" / "tube_01.png"))
    assert body.shape == (2, 3, 3)
    assert tuple(body[0, 0]) == (10, 20, 30)
    assert end.shape == (2, 2, 3)
    assert head.shape == (2, 2, 3)

"""Kiem tra mau bounding box tren anh annotated Step 1."""

import numpy as np

from main import InspectionService


def test_step1_roi_pass_ve_bounding_box_mau_trang(monkeypatch, tmp_path):
    """ROI PASS o Step 1 dung mau trang, ROI FAIL van dung mau do."""
    service = InspectionService.__new__(InspectionService)
    service.annotated_dir = tmp_path / "annotated"
    mau_bounding_box = []

    def ghi_bounding_box(image, point_1, point_2, color, thickness):
        mau_bounding_box.append(color)
        return image

    monkeypatch.setattr("main.cv2.rectangle", ghi_bounding_box)
    monkeypatch.setattr("main.cv2.imwrite", lambda path, image: True)

    service.create_annotated_image(
        raw_image=np.zeros((100, 100, 3), dtype=np.uint8),
        classifier_results=[],
        evaluation={
            "passed": False,
            "roi_results": [
                {
                    "passed": True,
                    "roi": {"x_min": 10, "y_min": 10, "x_max": 40, "y_max": 40},
                    "part_name": "",
                    "class_name": "OK",
                    "confidence": 0.99,
                },
                {
                    "passed": False,
                    "roi": {"x_min": 50, "y_min": 50, "x_max": 80, "y_max": 80},
                    "part_name": "",
                    "class_name": "NG",
                    "confidence": 0.20,
                },
            ],
        },
        event={
            "event_id": "event-step1",
            "cycle_id": "cycle-step1",
            "step": 1,
            "camera_side": "L",
            "tube_type": "No1",
        },
    )

    assert mau_bounding_box == [(255, 255, 255), (0, 0, 255)]

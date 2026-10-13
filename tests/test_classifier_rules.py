import numpy as np

from main import ClassifierResult, InspectionService, RoiRuleEngine


def test_classifier_rules_pass_when_each_part_predicts_expected_class(tmp_path):
    csv_path = tmp_path / "roi_rules.csv"
    csv_path.write_text(
        "inspection_step,roi_id,part_name,model_name,model_path,expected_class,"
        "compare_x_min,compare_y_min,compare_x_max,compare_y_max,confidence\n"
        "STEP_1,roi_body,BODY,AOL-Body,body.pt,Body OK,0,0,10,10,0.5\n"
        "STEP_1,roi_end,END,AOL-End,end.pt,End OK,10,0,20,10,0.5\n",
        encoding="utf-8",
    )

    engine = RoiRuleEngine(str(csv_path))
    result = engine.evaluate_classifiers(
        "STEP_1",
        [
            ClassifierResult("BODY", "AOL-Body", "Body OK", 0, 0.9, {}),
            ClassifierResult("END", "AOL-End", "End OK", 0, 0.8, {}),
        ],
    )

    assert result["passed"] is True
    assert all(item["passed"] for item in result["roi_results"])


def test_classifier_rules_fail_on_wrong_class_or_low_confidence(tmp_path):
    csv_path = tmp_path / "roi_rules.csv"
    csv_path.write_text(
        "inspection_step,roi_id,part_name,model_name,model_path,expected_class,"
        "compare_x_min,compare_y_min,compare_x_max,compare_y_max,confidence\n"
        "STEP_3,roi_head,HEAD,AOL-Head,head.pt,Head OK,0,0,10,10,0.5\n",
        encoding="utf-8",
    )

    engine = RoiRuleEngine(str(csv_path))
    result = engine.evaluate_classifiers(
        "STEP_3",
        [ClassifierResult("HEAD", "AOL-Head", "Head NG", 1, 0.99, {})],
    )

    assert result["passed"] is False
    assert result["roi_results"][0]["confidence"] == 0.99


def test_infer_classifiers_crops_each_part_once(tmp_path):
    csv_path = tmp_path / "roi_rules.csv"
    csv_path.write_text(
        "inspection_step,roi_id,part_name,model_name,model_path,expected_class,"
        "compare_x_min,compare_y_min,compare_x_max,compare_y_max,confidence\n"
        "STEP_1,roi_body,BODY,AOL-Body,body.pt,Body OK,1,2,11,22,0.5\n"
        "STEP_1,roi_end,END,AOL-End,end.pt,End OK,20,30,40,50,0.5\n",
        encoding="utf-8",
    )

    class FakeProbabilities:
        top1 = 0
        top1conf = 0.91

    class FakePrediction:
        probs = FakeProbabilities()
        names = {0: "OK", 1: "NG"}

    class FakeModel:
        def __init__(self):
            self.shapes = []

        def predict(self, source, **kwargs):
            self.shapes.append(source.shape[:2])
            return [FakePrediction()]

    body_model = FakeModel()
    end_model = FakeModel()
    service = object.__new__(InspectionService)
    service.rule_engine = RoiRuleEngine(str(csv_path))
    service.classifier_models = {"BODY": body_model, "END": end_model}
    service.config = {"model": {"device": "cpu", "classifier_imgsz": 224}}

    results = service.infer_classifiers(np.zeros((60, 70, 3), dtype=np.uint8))

    assert [result.part_name for result in results] == ["BODY", "END"]
    assert body_model.shapes == [(20, 10)]
    assert end_model.shapes == [(20, 20)]

from main import Detection, RoiRuleEngine


def test_aol_model_alias_matches_best1_rule(tmp_path):
    csv_path = tmp_path / "roi_rules.csv"
    csv_path.write_text(
        "inspection_step,roi_id,model_name,class_id,class_name,compare_x_min,compare_y_min,compare_x_max,compare_y_max,confidence,check_mode,min_count,max_count\n"
        "STEP_1,roi_001,best1,0,Loi khong dung dich,0,0,10,10,0.21,count,1,3\n",
        encoding="utf-8",
    )

    engine = RoiRuleEngine(str(csv_path))
    detections = [
        Detection(
            class_id=0,
            confidence=0.95,
            x1=2,
            y1=2,
            x2=8,
            y2=8,
        )
    ]

    result = engine.evaluate("STEP_1", "AOL", detections)

    assert result["passed"] is True
    assert len(result["roi_results"]) == 1

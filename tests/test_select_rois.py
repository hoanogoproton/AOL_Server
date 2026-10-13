from tools.select_rois import build_rows


def test_build_rows_writes_three_parts_for_both_steps():
    rows = build_rows([(1, 2, 10, 20), (30, 40, 50, 60), (70, 80, 90, 100)])

    assert len(rows) == 6
    assert [row["part_name"] for row in rows[:3]] == ["BODY", "END", "HEAD"]
    assert rows[0]["compare_x_max"] == "11"
    assert rows[0]["compare_y_max"] == "22"
    assert rows[3]["inspection_step"] == "STEP_3"

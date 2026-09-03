import main


def test_disabled_sender_simulates_success():
    """Serial disabled -> khong mo COM, tra True (che do gia lap)."""
    sender = main.ArduinoSignalSender(
        {"enabled": False, "port": "COM3", "baudrate": 115200}
    )

    assert sender.send_ng_signal() is True
    assert sender.is_connected() is False


class FakeSerial:
    written = []

    def __init__(self, *args, **kwargs):
        self.is_open = True
        FakeSerial.written = []

    def write(self, data):
        FakeSerial.written.append(data)
        return len(data)

    def flush(self):
        pass

    def reset_input_buffer(self):
        pass

    def reset_output_buffer(self):
        pass

    def close(self):
        self.is_open = False


def test_enabled_sender_writes_ng_signal(monkeypatch):
    """Serial enabled -> ghi dung b'0\\r\\n' ra cong."""
    monkeypatch.setattr(main.serial, "Serial", FakeSerial)

    sender = main.ArduinoSignalSender(
        {"enabled": True, "port": "COMX", "baudrate": 115200}
    )

    assert sender.send_ng_signal() is True
    assert FakeSerial.written == [b"0\r\n"]


def test_sender_retries_when_port_opens_late(monkeypatch):
    """Port lan mo dau loi -> tu retry va van gui duoc '0'."""
    monkeypatch.setattr(main.time, "sleep", lambda s: None)

    attempts = {"n": 0}

    class FlakySerial(FakeSerial):
        def __init__(self, *args, **kwargs):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise RuntimeError("port gone")

            super().__init__(*args, **kwargs)

    monkeypatch.setattr(main.serial, "Serial", FlakySerial)

    sender = main.ArduinoSignalSender(
        {"enabled": True, "port": "COMX", "baudrate": 115200}
    )

    assert sender.send_ng_signal() is True
    assert attempts["n"] == 2
    assert FakeSerial.written == [b"0\r\n"]


def test_sender_gives_up_after_retries(monkeypatch):
    """Port loi lien tuc qua retry_count lan -> tra False, khong crash."""
    monkeypatch.setattr(main.time, "sleep", lambda s: None)

    class DeadSerial(FakeSerial):
        def __init__(self, *args, **kwargs):
            raise RuntimeError("port gone")

    monkeypatch.setattr(main.serial, "Serial", DeadSerial)

    sender = main.ArduinoSignalSender(
        {
            "enabled": True,
            "port": "COMX",
            "baudrate": 115200,
            "retry_count": 3,
            "retry_interval_sec": 0.01,
        }
    )

    assert sender.send_ng_signal() is False
    assert sender.is_connected() is False

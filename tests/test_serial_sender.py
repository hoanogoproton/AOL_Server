import main


def test_disabled_sender_is_not_confirmed():
    """Serial disabled -> khong mo COM, khong co ACK (khong gia lap thanh cong)."""
    sender = main.ArduinoSignalSender(
        {"enabled": False, "port": "COM3", "baudrate": 115200}
    )

    assert sender.send_ng_signal() is False
    assert sender.is_connected() is False


class FakeSerial:
    written = []

    def __init__(self, *args, **kwargs):
        self.is_open = True
        self._next = b"Arduino san sang\n"
        FakeSerial.written = []

    @property
    def in_waiting(self):
        return len(self._next)

    def read(self, size=1):
        chunk = self._next[:size]
        self._next = self._next[size:]
        return chunk

    def write(self, data):
        FakeSerial.written.append(data)
        # Arduino chi ACK sau khi nhan lenh '0'.
        self._next = b"NHAN 0 - RELAY KICH HOAT\n"
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
    """Serial enabled -> ghi dung b'0\\r\\n' ra cong va nhan ACK."""
    monkeypatch.setattr(main.serial, "Serial", FakeSerial)

    sender = main.ArduinoSignalSender(
        {"enabled": True, "port": "COMX", "baudrate": 115200}
    )

    assert sender.send_ng_signal() is True
    assert FakeSerial.written == [b"0\r\n"]
    assert sender.get_status()["last_ng_confirmed"] is True


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


def test_sender_missing_ack_is_unconfirmed(monkeypatch):
    """Da ghi lenh nhung khong co ACK -> False, khong tu gui lai."""
    monkeypatch.setattr(main.time, "sleep", lambda s: None)

    class NoAckSerial(FakeSerial):
        def write(self, data):
            FakeSerial.written.append(data)
            self._next = b""
            return len(data)

    monkeypatch.setattr(main.serial, "Serial", NoAckSerial)

    sender = main.ArduinoSignalSender(
        {
            "enabled": True,
            "port": "COMX",
            "baudrate": 115200,
            "ack_timeout_sec": 0.05,
        }
    )

    assert sender.send_ng_signal() is False
    assert FakeSerial.written == [b"0\r\n"]
    assert sender.get_status()["last_ng_confirmed"] is False

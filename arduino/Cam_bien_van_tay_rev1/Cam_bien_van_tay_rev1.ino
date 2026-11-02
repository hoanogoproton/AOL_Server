#include <SoftwareSerial.h>
#include <Adafruit_Fingerprint.h>
#include <string.h>

#define RELAY_PIN 4  // Relay active LOW

SoftwareSerial mySerial(2, 3); // Arduino RX, TX
Adafruit_Fingerprint finger = Adafruit_Fingerprint(&mySerial);

void setup() {
  Serial.begin(9600);

  pinMode(RELAY_PIN, OUTPUT);
  digitalWrite(RELAY_PIN, HIGH); // Relay ban đầu tắt

  finger.begin(57600);

  if (finger.verifyPassword()) {
    Serial.println("Da ket noi cam bien van tay");
  } else {
    Serial.println("Khong ket noi duoc cam bien");
    while (true);
  }

  Serial.println("Arduino san sang");
}

void loop() {
  readSerialCommand();

  // Kiểm tra vân tay
  int id = getFingerprintID();

  if (id > 0) {
    digitalWrite(RELAY_PIN, HIGH); // Relay active LOW: HIGH là tắt

    Serial.print("VAN TAY DUNG - ID: ");
    Serial.print(id);
    Serial.println(" - RELAY TAT");
    delay(500);
  }

  delay(50);
}

void readSerialCommand() {
  static char line[16];
  static byte index = 0;

  while (Serial.available() > 0) {
    char c = Serial.read();

    // Bỏ qua ký tự CR trong chuỗi 0\r\n
    if (c == '\r') {
      continue;
    }

    // LF kết thúc dòng
    if (c == '\n') {
      line[index] = '\0';

      if (strcmp(line, "0") == 0) {
        digitalWrite(RELAY_PIN, LOW); // Kích hoạt relay

        Serial.println("NHAN 0 - RELAY KICH HOAT");
      }

      // Xóa bộ đệm để nhận lệnh tiếp theo
      index = 0;
      line[0] = '\0';
    } else {
      // Lưu ký tự nếu còn chỗ
      if (index < sizeof(line) - 1) {
        line[index++] = c;
      } else {
        // Dòng quá dài: bỏ dòng đó
        index = 0;
      }
    }
  }
}

int getFingerprintID() {
  uint8_t p = finger.getImage();
  if (p != FINGERPRINT_OK) {
    return -1;
  }

  p = finger.image2Tz();
  if (p != FINGERPRINT_OK) {
    return -1;
  }

  p = finger.fingerFastSearch();
  if (p != FINGERPRINT_OK) {
    return -1;
  }

  return finger.fingerID;
}
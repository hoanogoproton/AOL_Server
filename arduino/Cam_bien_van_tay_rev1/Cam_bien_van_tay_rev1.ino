#include <SoftwareSerial.h>
#include <Adafruit_Fingerprint.h>

#define RELAY_PIN 4   // Relay active LOW

SoftwareSerial mySerial(2, 3); // Arduino RX, TX
Adafruit_Fingerprint finger = Adafruit_Fingerprint(&mySerial);

void setup() {
  Serial.begin(9600);

  pinMode(RELAY_PIN, OUTPUT);

  // Relay ban đầu tắt
  digitalWrite(RELAY_PIN, HIGH);

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
  // Kiểm tra lệnh từ PC
  readSerialCommand();

  // Kiểm tra vân tay
  int id = getFingerprintID();

  if (id > 0) {
    // Vân tay đúng -> chân 4 HIGH
    digitalWrite(RELAY_PIN, HIGH);

    Serial.print("VAN TAY DUNG - ID: ");
    Serial.print(id);
    Serial.println(" - CHAN 4 = HIGH");

    delay(500);
  }

  delay(50);
}

void readSerialCommand() {
  static char lastChar = 0;

  while (Serial.available() > 0) {
    char c = Serial.read();

    // Chuyển chữ thường thành chữ hoa
    if (c >= 'a' && c <= 'z') {
      c = c - 'a' + 'A';
    }

    // Nhận chuỗi "NG"
    if (lastChar == 'N' && c == 'G') {
      digitalWrite(RELAY_PIN, LOW);
      Serial.println("NHAN NG TU PC - CHAN 4 = LOW");

      lastChar = 0;
    } else {
      lastChar = c;
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

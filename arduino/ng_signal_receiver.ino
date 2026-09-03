// Nhan tin hieu NG tu AI Inspection Server qua Serial (115200 baud).
//
// Giao thuc don gian:
//   AI Server gui ky tu '0' (kem \r\n) moi khi Step 3 phat hien san pham loi.
//   Khong can tra ACK — server gui la xong (fire-and-forget).
//
// Cach hoat dong: nhan '0' -> bat relay bao loi trong 2 giay roi tat.

const unsigned long NG_RELAY_DURATION_MS = 2000;
const int NG_RELAY_PIN = 13;  // Doi thanh pin relay that cua ban

void setup() {
  Serial.begin(115200);
  pinMode(NG_RELAY_PIN, OUTPUT);
  digitalWrite(NG_RELAY_PIN, LOW);
}

void loop() {
  while (Serial.available() > 0) {
    char c = Serial.read();

    // Bo qua ky tu ket thuc dong (\r, \n)
    if (c == '\r' || c == '\n') {
      continue;
    }

    if (c == '0') {
      // Step 3 NG: bat relay bao loi
      digitalWrite(NG_RELAY_PIN, HIGH);
      delay(NG_RELAY_DURATION_MS);
      digitalWrite(NG_RELAY_PIN, LOW);
    }
    // Ky tu khac: bo qua (giao thuc chi co '0')
  }
}

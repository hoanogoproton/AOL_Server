
#include <SoftwareSerial.h>

SoftwareSerial fp(2, 3);   // UNO RX=2, TX=3

#define TIMES 5
#define SERIAL_TIMEOUT 30000UL

// =====================================================
// SEND R558 COMMAND
// =====================================================
void sendCmd(byte *d, byte n)
{
  word len = n + 2;
  word sum = 1 + (len >> 8) + (len & 0xFF);

  for (byte i = 0; i < n; i++)
    sum += d[i];

  fp.write(0xEF);
  fp.write(0x01);

  fp.write(0xFF);
  fp.write(0xFF);
  fp.write(0xFF);
  fp.write(0xFF);

  fp.write(0x01);

  fp.write(len >> 8);
  fp.write(len & 0xFF);

  for (byte i = 0; i < n; i++)
    fp.write(d[i]);

  fp.write(sum >> 8);
  fp.write(sum & 0xFF);
}

// =====================================================
// READ PACKET
// =====================================================
byte readPacket(byte *r, byte maxLen, unsigned long timeout)
{
  byte n = 0;
  unsigned long t = millis();

  while (millis() - t < timeout)
  {
    if (!fp.available())
      continue;

    byte b = fp.read();

    if (n == 0 && b != 0xEF)
      continue;

    if (n == 1 && b != 0x01)
    {
      n = 0;
      continue;
    }

    if (n < maxLen)
      r[n++] = b;

    if (n >= 9)
    {
      word len = ((word)r[7] << 8) | r[8];

      if (n >= 9 + len)
        return n;
    }
  }

  return 0;
}

// =====================================================
// CLEAR RX
// =====================================================
void clearRX()
{
  while (fp.available())
    fp.read();
}

// =====================================================
// READ SERIAL LINE
// Read complete line, handle CR / LF / CRLF
// =====================================================
bool readLine(char *buffer, byte maxLen, unsigned long timeout)
{
  byte n = 0;
  unsigned long t = millis();

  while (millis() - t < timeout)
  {
    if (!Serial.available())
      continue;

    char c = Serial.read();
    t = millis();

    // Ignore CR
    if (c == '\r')
      continue;

    // End of line
    if (c == '\n')
    {
      buffer[n] = '\0';
      return true;
    }

    // Backspace
    if (c == '\b' || c == 127)
    {
      if (n > 0)
        n--;

      continue;
    }

    // Store character
    if (n < maxLen - 1)
    {
      buffer[n++] = c;
    }
    else
    {
      Serial.println(F("INPUT TOO LONG"));
      return false;
    }
  }

  Serial.println(F("INPUT TIMEOUT"));
  return false;
}

// =====================================================
// INPUT ID
// =====================================================
bool inputID(word &id)
{
  char buffer[6];

  Serial.println(F("NHAP ID (1-65535):"));

  if (!readLine(buffer, sizeof(buffer), SERIAL_TIMEOUT))
    return false;

  // Empty input
  if (buffer[0] == '\0')
  {
    Serial.println(F("ID KHONG HOP LE"));
    return false;
  }

  unsigned long value = 0;

  for (byte i = 0; buffer[i] != '\0'; i++)
  {
    if (buffer[i] < '0' || buffer[i] > '9')
    {
      Serial.println(F("CHI NHAP SO!"));
      return false;
    }

    value = value * 10 + (buffer[i] - '0');

    if (value > 65535)
    {
      Serial.println(F("ID QUA GIOI HAN"));
      return false;
    }
  }

  if (value == 0)
  {
    Serial.println(F("ID PHAI LON HON 0"));
    return false;
  }

  id = (word)value;

  Serial.print(F("SELECTED ID: "));
  Serial.println(id);

  return true;
}

// =====================================================
// INPUT YES / NO
// =====================================================
bool confirmDelete()
{
  char buffer[10];

  Serial.println(F("XAC NHAN XOA? (Y/N):"));

  if (!readLine(buffer, sizeof(buffer), SERIAL_TIMEOUT))
    return false;

  if (buffer[0] == 'Y' || buffer[0] == 'y')
    return true;

  return false;
}

// =====================================================
// SET LOGIC MODE = 1
// =====================================================
bool setLogicMode()
{
  byte c[] = {0x0E, 0x03, 0x01};

  clearRX();
  sendCmd(c, 3);

  byte r[20];

  if (!readPacket(r, 20, 3000))
    return false;

  return r[9] == 0x00;
}

// =====================================================
// DELETE ID
// =====================================================
void deleteID(word id)
{
  Serial.println();
  Serial.print(F("DELETE ID "));
  Serial.println(id);

  byte c[] = {
    0x0C,
    (byte)(id >> 8), (byte)(id & 0xFF),
    0x00, 0x01
  };

  clearRX();
  sendCmd(c, 5);

  byte r[20];

  if (!readPacket(r, 20, 3000))
  {
    Serial.println(F("TIMEOUT"));
    return;
  }

  if (r[9] == 0x00)
    Serial.println(F("DELETE OK"));
  else
    Serial.println(F("DELETE FAIL"));
}

// =====================================================
// ADD ID - 5 LAN
// =====================================================
void addID(word id)
{
  Serial.println();
  Serial.print(F("=== ADD FINGERPRINT ID "));
  Serial.print(id);
  Serial.println(F(" ==="));

  // Logic Mode 1
  if (!setLogicMode())
  {
    Serial.println(F("SET MODE FAIL"));
    return;
  }

  // AutoEnroll
  byte c[] = {
    0x31,
    (byte)(id >> 8), (byte)(id & 0xFF),
    TIMES,
    0x00, 0x08
  };

  clearRX();
  sendCmd(c, 6);

  byte r[30];

  while (true)
  {
    if (!readPacket(r, 30, 15000))
    {
      Serial.println(F("TIMEOUT"));
      return;
    }

    byte code = r[9];
    byte step = r[10];
    byte no   = r[11];

    // Error
    if (code != 0x00)
    {
      Serial.print(F("ERROR 0x"));
      Serial.println(code, HEX);
      return;
    }

    // Start
    if (step == 0x00)
    {
      Serial.println(F("[1/5] DAT NGON TAY"));
    }

    // Image OK
    else if (step == 0x01)
    {
      Serial.print(F("["));
      Serial.print(no);
      Serial.println(F("/5] DA NHAN"));
    }

    // Feature OK
    else if (step == 0x02)
    {
      Serial.print(F("["));
      Serial.print(no);
      Serial.println(F("/5] XU LY OK"));
    }

    // Remove finger
    else if (step == 0x03)
    {
      Serial.print(F("["));
      Serial.print(no);
      Serial.println(F("/5] NHA NGON TAY"));

      if (no < TIMES)
      {
        Serial.print(F("["));
        Serial.print(no + 1);
        Serial.println(F("/5] DAT NGON TAY"));
      }
    }

    // Merge
    else if (step == 0x04)
    {
      Serial.println(F("TAO TEMPLATE..."));
    }

    // Duplicate check
    else if (step == 0x05)
    {
      Serial.println(F("KIEM TRA TRUNG..."));
    }

    // Store
    else if (step == 0x06)
    {
      Serial.print(F("ADD SUCCESS - ID "));
      Serial.println(id);
      return;
    }
  }
}

// =====================================================
// CHECK - SEARCH ALL ID
// =====================================================
void checkID()
{
  Serial.println();
  Serial.println(F("DAT NGON TAY..."));

  byte c[] = {
    0x32,
    0x03,
    0xFF, 0xFF,
    0x00, 0x00
  };

  clearRX();
  sendCmd(c, 6);

  byte r[30];

  while (true)
  {
    if (!readPacket(r, 30, 10000))
    {
      Serial.println(F("TIMEOUT"));
      return;
    }

    byte code = r[9];
    byte step = r[10];

    if (code == 0x09)
    {
      Serial.println(F("KHONG TIM THAY"));
      return;
    }

    if (code != 0x00)
    {
      Serial.print(F("ERROR 0x"));
      Serial.println(code, HEX);
      return;
    }

    // Match
    if (step == 0x05)
    {
      word id =
        ((word)r[11] << 8) |
        r[12];

      word score =
        ((word)r[13] << 8) |
        r[14];

      Serial.print(F("MATCH ID="));
      Serial.println(id);

      Serial.print(F("SCORE="));
      Serial.println(score);

      return;
    }
  }
}

// =====================================================
// STATUS
// =====================================================
void status()
{
  byte c[] = {0x0F};

  clearRX();
  sendCmd(c, 1);

  byte r[40];

  if (!readPacket(r, 40, 3000))
  {
    Serial.println(F("R558 TIMEOUT"));
    return;
  }

  if (r[9] == 0x00)
    Serial.println(F("R558 OK"));
  else
    Serial.println(F("R558 ERROR"));
}

// =====================================================
// PROCESS COMMAND
// =====================================================
void processCommand(char *cmd)
{
  // Ignore empty line
  if (cmd[0] == '\0')
    return;

  // Convert lowercase to uppercase
  for (byte i = 0; cmd[i] != '\0'; i++)
  {
    if (cmd[i] >= 'a' && cmd[i] <= 'z')
      cmd[i] -= 32;
  }

  word id;

  // ADD
  if (cmd[0] == 'A' && cmd[1] == '\0')
  {
    Serial.println();
    Serial.println(F("=== ADD FINGERPRINT ==="));

    if (inputID(id))
      addID(id);
  }

  // DELETE
  else if (cmd[0] == 'D' && cmd[1] == '\0')
  {
    Serial.println();
    Serial.println(F("=== DELETE FINGERPRINT ==="));

    if (inputID(id))
    {
      Serial.print(F("DELETE ID "));
      Serial.println(id);

      if (confirmDelete())
        deleteID(id);
      else
        Serial.println(F("CANCEL DELETE"));
    }
  }

  // CHECK
  else if (cmd[0] == 'C' && cmd[1] == '\0')
  {
    checkID();
  }

  // STATUS
  else if (cmd[0] == 'S' && cmd[1] == '\0')
  {
    status();
  }

  else
  {
    Serial.println(F("UNKNOWN COMMAND"));
    Serial.println(F("A=ADD C=CHECK D=DELETE S=STATUS"));
  }
}

// =====================================================
// SETUP
// =====================================================
void setup()
{
  Serial.begin(9600);
  fp.begin(57600);

  delay(500);

  Serial.println(F("R558 V9.1"));
  Serial.println(F("A=ADD  C=CHECK  D=DELETE  S=STATUS"));
}

// =====================================================
// LOOP
// =====================================================
void loop()
{
  if (Serial.available())
  {
    char command[10];

    // Read complete command line
    if (readLine(command, sizeof(command), SERIAL_TIMEOUT))
    {
      processCommand(command);
    }
  }
}

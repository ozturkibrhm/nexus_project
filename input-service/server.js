const express = require('express');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = 4000; // veya ihtiyacınıza göre farklı bir port

app.use(express.static('public'));
app.use(express.json());

const outputDir = '/shared-data';
const outputFile = path.join(outputDir, 'secrets.json');

// Output klasörünün varlığını kontrol eder, yoksa oluşturur
function ensureOutputDir() {
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }
}

// Mevcut secrets.json dosyasını okur ve parse eder
function readSecrets() {
  try {
    if (fs.existsSync(outputFile)) {
      const data = fs.readFileSync(outputFile, 'utf8');
      return JSON.parse(data);
    }
  } catch (err) {
    console.error('Error reading secrets file:', err);
  }
  return {};
}

// secrets nesnesini JSON formatında dosyaya yazar
function writeSecrets(secrets) {
  try {
    ensureOutputDir();
    fs.writeFileSync(outputFile, JSON.stringify(secrets, null, 2), 'utf8');
  } catch (err) {
    console.error('Error writing secrets file:', err);
    throw err;
  }
}

// Form verisini alıp JSON dosyasına yazar
app.post('/submit', (req, res) => {
  const { ip, port, username, password } = req.body;
  if (!ip || !port || !username || !password) {
    return res.status(400).send('Eksik form verisi.');
  }

  const safeKey = `${ip}_${port}`.replace(/[^a-zA-Z0-9]/g, '_');
  try {
    const secrets = readSecrets();
    secrets[safeKey] = { ip, port, username, password };
    writeSecrets(secrets);
    res.send('Sunucu bilgileri kaydedildi ve secrets.json güncellendi.');
  } catch (error) {
    console.error('submit POST error:', error);
    res.status(500).send('Veri kaydedilemedi veya export başarısız.');
  }
  
});


// Tüm anahtarları listeler
app.get('/all-secrets', (req, res) => {
  const secrets = readSecrets();
  res.json(Object.keys(secrets));
});

// Belirli bir anahtara ait veriyi döner
app.get('/secret/:key', (req, res) => {
  const safeKey = req.params.key.replace(/[^a-zA-Z0-9]/g, '_');
  const secrets = readSecrets();
  if (secrets[safeKey]) {
    res.json(secrets[safeKey]);
  } else {
    res.status(404).send('Anahtar bulunamadı.');
  }
});

app.listen(PORT, () => {
  console.log(`Input Service çalışıyor: http://localhost:${PORT}`);
});

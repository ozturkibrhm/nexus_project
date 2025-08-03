FROM node:18

# Çalışma dizini
WORKDIR /app

# package.json ve package-lock.json kopyala
COPY package*.json ./

# Gerekli modülleri yükle
RUN npm install

# Uygulama dosyalarını kopyala
COPY . .

# 4000 portunu expose et (senin app bu portta çalışıyor)
EXPOSE 4000

# Sunucuyu başlat
CMD ["node", "server.js"]
	
# Stage 1: Build
FROM node:22-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
ARG VITE_API_BASE_URL
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL
# Shared config-write token (ADR-013). Baked into the bundle so the config form can send the
# X-Config-Token header on the droplet; blank locally = no header (the backend gate is a no-op).
ARG VITE_CONFIG_WRITE_TOKEN
ENV VITE_CONFIG_WRITE_TOKEN=$VITE_CONFIG_WRITE_TOKEN
RUN npm run build

# Stage 2: Serve
FROM nginx:alpine
COPY --from=builder /app/dist /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
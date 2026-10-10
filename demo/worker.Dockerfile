# Ingestion worker. The dist bundle is self-contained (shared code is inlined),
# so no npm install is needed. Build it first: `npm run build`.
FROM node:24-slim
WORKDIR /app
COPY packages/ingestion-worker/dist ./dist
ENV NODE_ENV=production PORT=8080
USER node
EXPOSE 8080
CMD ["node", "dist/main.js"]

# PTY Drive Backend para Render + Supabase

## Archivos
- `server.js`: Backend Node.js + Express + Socket.io + PostgreSQL.
- `package.json`: Dependencias y start command.
- `.env.example`: Variables de entorno para Render.

## Render
Build Command:
```bash
npm install
```

Start Command:
```bash
npm start
```

## Variables obligatorias en Render
```env
NODE_ENV=production
DATABASE_URL=postgresql://...
JWT_SECRET=...
CORS_ORIGIN=*
ADMIN_EMAIL=...
ADMIN_PASSWORD=...
ADMIN_NAME=Administrador
RIDE_EXPIRE_MINUTES=10
SOS_COOLDOWN_MINUTES=5
```

## Endpoints principales
- `GET /api/health`
- `POST /api/auth/register`
- `POST /api/auth/login`
- `GET /api/auth/me`
- `POST /api/rides`
- `GET /api/rides`
- `PATCH /api/rides/:id/accept`
- `PATCH /api/carrera-lite/:id/accept`
- `PATCH /api/rides/:id/start`
- `PATCH /api/rides/:id/complete`
- `PATCH /api/rides/:id/cancel`
- `POST /api/locations`
- `POST /api/sos`
- `GET /api/admin/sos`
- `GET /api/admin/rides`
- `GET /api/admin/users`
- `GET /api/chats/:rideId/messages`
- `POST /api/chats/:rideId/messages`

## Nota de seguridad
No subas `.env` a GitHub.

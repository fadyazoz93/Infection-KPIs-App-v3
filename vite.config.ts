import path from 'path';
import { defineConfig, loadEnv } from 'vite';


export default defineConfig(({ mode }) => {
    const env = loadEnv(mode, '.', '');
    
    // Ensure development environment variables are loaded into process.env
    process.env.TURSO_DATABASE_URL = env.TURSO_DATABASE_URL || process.env.TURSO_DATABASE_URL;
    process.env.TURSO_AUTH_TOKEN = env.TURSO_AUTH_TOKEN || process.env.TURSO_AUTH_TOKEN;
    process.env.VITE_API_URL = env.VITE_API_URL || process.env.VITE_API_URL;
    process.env.VITE_SHEET_ID = env.VITE_SHEET_ID || process.env.VITE_SHEET_ID;

    return {
      server: {
        port: 3000,
        host: '0.0.0.0',
      },
      plugins: [
        {
          name: 'api-server',
          configureServer(server) {
            server.middlewares.use('/api', async (req, res) => {
              try {
                const { default: handler } = await server.ssrLoadModule('/api/index.ts');
                await handler(req, res);
              } catch (err: any) {
                console.error("Vite API middleware error:", err);
                res.statusCode = 500;
                res.setHeader('Content-Type', 'application/json');
                res.end(JSON.stringify({ success: false, error: err.message || String(err) }));
              }
            });
          }
        }
      ],
      define: {
        'process.env.API_KEY': JSON.stringify(env.GEMINI_API_KEY),
        'process.env.GEMINI_API_KEY': JSON.stringify(env.GEMINI_API_KEY)
      },
      resolve: {
        alias: {
          '@': path.resolve(__dirname, '.'),
        }
      }
    };
});

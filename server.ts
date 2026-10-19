import { createServer } from 'http';
import app from './api/index.js';

const port = 3000;
const server = createServer((req, res) => {
  app(req, res);
});

server.listen(port, () => {
  console.log(`Server running on port ${port}`);
});

import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// https://vitejs.dev/config/
export default defineConfig({
  base: '/vizard/preview/react-typescript-chartjs-area+bar+doughnut+line+pie/',
  plugins: [react()],
});

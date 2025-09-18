import { performance, PerformanceObserver } from 'perf_hooks';

interface PerfMetric {
  count: number;
  totalTime: number;
  avgTime: number;
}

interface PerfStats {
  [key: string]: PerfMetric;
}

class PerfTracker {
  private metrics: PerfStats = {};

  private globalMetric: string | null = null;

  public constructor() {
    const obs = new PerformanceObserver((items) => {
      for (const entry of items.getEntries()) {
        const { name } = entry;
        if (!this.metrics[name]) {
          this.metrics[name] = { count: 0, totalTime: 0, avgTime: 0 };
        }
        const m = this.metrics[name];
        m.count++;
        m.totalTime += entry.duration;
        m.avgTime = m.totalTime / m.count;
      }
    });
    obs.observe({ entryTypes: ['measure'] });
  }

  public start(name: string, global: boolean = false): { end: () => void } {
    const uid = `${name}-${performance.now()}`;
    const startMark = `${uid}-start`;
    const endMark = `${uid}-end`;
    performance.mark(startMark);

    if (global && !this.globalMetric) {
      this.globalMetric = name;
    }

    let ended = false;

    return {
      end: () => {
        if (ended) return;
        performance.mark(endMark);
        performance.measure(name, startMark, endMark);
        ended = true;
      }
    };
  }

  public printReport() {
    console.log('\n🚀 PERFORMANCE REPORT 🚀\n');
    console.log('═'.repeat(90));

    const sorted = Object.entries(this.metrics)
      .sort(([, a], [, b]) => b.totalTime - a.totalTime);

    if (!sorted.length) {
      console.log('No performance data collected.');
      return;
    }

    let totalTime: number = 0;

    if (this.globalMetric) {
      totalTime = this.metrics[this.globalMetric]?.totalTime;
    } else {
      totalTime = sorted.reduce((sum, [, m]) => sum + m.totalTime, 0);
    }

    console.log(`⏱️  TOTAL TIME: ${totalTime.toFixed(2)}ms\n`);

    sorted.forEach(([name, m]) => {
      const pct = totalTime > 0 ? (m.totalTime / totalTime * 100) : 0;
      console.log(`  ${name.padEnd(40)} │ ${m.totalTime.toFixed(2).padStart(8)}ms │ ${m.avgTime.toFixed(2).padStart(7)}ms avg │ ${pct.toFixed(1).padStart(5)}% │ ${m.count.toString().padStart(4)} calls`);
    });

    console.log('═'.repeat(90));
    console.log('🎯 End of Performance Report\n');
  }
}

export const perfTracker = new PerfTracker();

<script>
import { Line, mixins } from 'vue-chartjs';

export default {
  extends: Line,
  props: {
    data: {
      type: Object,
      default: null,
    },
    options: {
      type: Object,
      default: null,
    },
  },
  mixins: [mixins.reactiveProp],
  mounted() {
    const data = {
      labels: ['January', 'February'],
      datasets: [
        {
          label: 'Data One',
          backgroundColor: '#f87979',
          data: [40, 20],
        },
      ],
    };
    this.renderChart(data, {
      responsive: false
    });
  },
};
</script>

// ChartJS extension rounded bar chart
// https://codepen.io/jedtrow/full/ygRYgo
function draw() {
  const { ctx } = this._chart;
  const vm = this._view;
  let { borderWidth } = vm;

  let left;
  let right;
  let top;
  let bottom;
  let signX;
  let signY;
  let borderSkipped;
  let radius;

  // If radius is less than 0 or is large enough to cause drawing errors a max
  //      radius is imposed. If cornerRadius is not defined set it to 0.
  let { cornerRadius } = this._chart.config.options;
  if (cornerRadius < 0) {
    cornerRadius = 0;
  }

  if (typeof cornerRadius === 'undefined') {
    cornerRadius = 0;
  }

  if (!vm.horizontal) {
    // bar
    left = vm.x - vm.width / 2;
    right = vm.x + vm.width / 2;
    top = vm.y;
    bottom = vm.base;
    signX = 1;
    signY = bottom > top ? 1 : -1;
    borderSkipped = vm.borderSkipped || 'bottom';
  } else {
    // horizontal bar
    left = vm.base;
    right = vm.x;
    top = vm.y - vm.height / 2;
    bottom = vm.y + vm.height / 2;
    signX = right > left ? 1 : -1;
    signY = 1;
    borderSkipped = vm.borderSkipped || 'left';
  }

  // Canvas doesn't allow us to stroke inside the width so we can
  // adjust the sizes to fit if we're setting a stroke on the line
  if (borderWidth) {
    // borderWidth shold be less than bar width and bar height.
    const barSize = Math.min(Math.abs(left - right), Math.abs(top - bottom));
    borderWidth = borderWidth > barSize ? barSize : borderWidth;
    const halfStroke = borderWidth / 2;
    // Adjust borderWidth when bar top position is near vm.base(zero).
    const borderLeft =
      left + (borderSkipped !== 'left' ? halfStroke * signX : 0);
    const borderRight =
      right + (borderSkipped !== 'right' ? -halfStroke * signX : 0);
    const borderTop = top + (borderSkipped !== 'top' ? halfStroke * signY : 0);
    const borderBottom =
      bottom + (borderSkipped !== 'bottom' ? -halfStroke * signY : 0);
    // not become a vertical line?
    if (borderLeft !== borderRight) {
      top = borderTop;
      bottom = borderBottom;
    }
    // not become a horizontal line?
    if (borderTop !== borderBottom) {
      left = borderLeft;
      right = borderRight;
    }
  }

  ctx.beginPath();
  ctx.fillStyle = vm.backgroundColor;
  ctx.strokeStyle = vm.borderColor;
  ctx.lineWidth = borderWidth;

  // Corner points, from bottom-left to bottom-right clockwise
  // | 1 2 |
  // | 0 3 |
  const corners = [[left, bottom], [left, top], [right, top], [right, bottom]];

  // Find first (starting) corner with fallback to 'bottom'
  const borders = ['bottom', 'left', 'top', 'right'];
  let startCorner = borders.indexOf(borderSkipped, 0);
  if (startCorner === -1) {
    startCorner = 0;
  }

  function cornerAt(index) {
    return corners[(startCorner + index) % 4];
  }

  // Draw rectangle from 'startCorner'
  let corner = cornerAt(0);
  ctx.moveTo(corner[0], corner[1]);

  for (let i = 1; i < 4; i += 1) {
    corner = cornerAt(i);
    let nextCornerId = i + 1;
    if (nextCornerId === 4) {
      nextCornerId = 0;
    }

    const width = corners[2][0] - corners[1][0];
    const height = corners[0][1] - corners[1][1];
    const x = corners[1][0];
    const y = corners[1][1];

    radius = cornerRadius;
    // Fix radius being too large
    if (radius > Math.abs(height) / 2) {
      radius = Math.floor(Math.abs(height) / 2);
    }
    if (radius > Math.abs(width) / 2) {
      radius = Math.floor(Math.abs(width) / 2);
    }

    if (height < 0) {
      // Negative values in a standard bar chart
      const xTl = x;
      const xTr = x + width;
      const yTl = y + height;
      const yTr = y + height;

      const xBl = x;
      const xBr = x + width;
      const yBl = y;
      const yBr = y;

      // Draw
      ctx.moveTo(xBl + radius, yBl);
      ctx.lineTo(xBr - radius, yBr);
      ctx.quadraticCurveTo(xBr, yBr, xBr, yBr - radius);
      ctx.lineTo(xTr, yTr + radius);
      ctx.quadraticCurveTo(xTr, yTr, xTr - radius, yTr);
      ctx.lineTo(xTl + radius, yTl);
      ctx.quadraticCurveTo(xTl, yTl, xTl, yTl + radius);
      ctx.lineTo(xBl, yBl - radius);
      ctx.quadraticCurveTo(xBl, yBl, xBl + radius, yBl);
    } else if (width < 0) {
      // Negative values in a horizontal bar chart
      const xTl = x + width;
      const xTr = x;
      const yTl = y;
      const yTr = y;

      const xBl = x + width;
      const xBr = x;
      const yBl = y + height;
      const yBr = y + height;

      // Draw
      ctx.moveTo(xBl + radius, yBl);
      ctx.lineTo(xBr - radius, yBr);
      ctx.quadraticCurveTo(xBr, yBr, xBr, yBr - radius);
      ctx.lineTo(xTr, yTr + radius);
      ctx.quadraticCurveTo(xTr, yTr, xTr - radius, yTr);
      ctx.lineTo(xTl + radius, yTl);
      ctx.quadraticCurveTo(xTl, yTl, xTl, yTl + radius);
      ctx.lineTo(xBl, yBl - radius);
      ctx.quadraticCurveTo(xBl, yBl, xBl + radius, yBl);
    } else {
      // Positive Value
      ctx.moveTo(x + radius, y);
      ctx.lineTo(x + width - radius, y);
      ctx.quadraticCurveTo(x + width, y, x + width, y + radius);
      ctx.lineTo(x + width, y + height - radius);
      ctx.quadraticCurveTo(
        x + width,
        y + height,
        x + width - radius,
        y + height
      );
      ctx.lineTo(x + radius, y + height);
      ctx.quadraticCurveTo(x, y + height, x, y + height - radius);
      ctx.lineTo(x, y + radius);
      ctx.quadraticCurveTo(x, y, x + radius, y);
    }
  }

  ctx.fill();
  if (borderWidth) {
    ctx.stroke();
  }
}

export default {
  draw
};

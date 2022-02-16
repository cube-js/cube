import styled from 'styled-components';

const POSITIONS = [
  [49, 0.5],
  [0, 25],
  [49, 49],
  [98, 25],
];

const cubeImage = `data:image/svg+xml,%3Csvg width='36' height='41' fill='none' xmlns='http://www.w3.org/2000/svg'%3E%3Cpath d='M35.899 10.351l-18 10.25L.1 10.25l18-10.25L35.9 10.351z' fill='%23FAFAFF'/%3E%3Cpath d='M18 41L0 30.75l.101-20.5L18 20.5' fill='%23E5E5F6'/%3E%3Cpath d='M36 30.75L18 41V20.6l17.899-10.25L36 30.75z' fill='%23C0C0EA'/%3E%3C/svg%3E`;

function pos(index: number) {
  return `transform: translate(${POSITIONS[index][0]}%, ${POSITIONS[index][1]}%);`;
}

type TRawImgProps = {
  index: number;
};

const RawImg = styled.img<TRawImgProps>(
  ({ index }) => `
  display: block;
  position: absolute;
  width: 50%;
  height: 50%;
  
  ${
    index != null
      ? `
  animation-name: dice${index};
  animation-duration: 2s;
  animation-iteration-count: infinite;
  animation-timing-function: ease;
  z-index: 0;
  
  @keyframes dice${index} {
    ${
      index === 0
        ? `
    from {
      ${pos(0)}
      z-index: 0;
    }
    
    25% {
      ${pos(0)}
      z-index: 0;
    }
    
    50% {
      ${pos(1)}
      z-index: 0;
    }
    
    51% {
      z-index: 1;
    }
    
    75% {
      z-index: 1;
    }

    to {
      ${pos(1)}
      z-index: 1;
    }
    `
        : ''
    }
    
    ${
      index === 1
        ? `
    from {
      ${pos(1)}
      z-index: 3;
    }
    
    25% {
      ${pos(2)}
      z-index: 3;
    }
    
    75% {
      ${pos(2)}
      z-index: 2;
    }
    
    to {
      ${pos(3)}
      z-index: 1;
    }
    `
        : ''
    }
    
    ${
      index === 2
        ? `
    from {
      ${pos(3)}
      z-index: 1;
    }
    
    50% {
      ${pos(3)}
      z-index: 0;
    }
    
    75% {
      ${pos(0)}
      z-index: 0;
    }

    to {
      ${pos(0)}
      z-index: 0;
    }
    `
        : ''
    }
  }
  `
      : ''
  }
`
);

const Cube = (props: any) => {
  return <RawImg role="presentation" src={cubeImage} alt="" {...props} />;
};

const Centered = styled.div`
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
`;

export function CubeLoader({ size = 80, full = true }) {
  const img = (
    <div
      data-testid="cube-loader"
      key="loader"
      role="img"
      aria-label="Loading animation"
      style={{
        position: 'relative',
        width: size,
        height: size * 1.1388888889,
        margin: '0 auto',
      }}
    >
      <Cube style={{ transform: 'translate(0%, 72.5%)' }} />
      <Cube style={{ transform: 'translate(98%, 72.5%)' }} />
      <Cube style={{ transform: 'translate(49%, 96.5%)' }} />
      <Cube index={0} />
      <Cube index={1} />
      <Cube index={2} />
    </div>
  );

  if (full) {
    return <Centered>{img}</Centered>;
  }

  return img;
}

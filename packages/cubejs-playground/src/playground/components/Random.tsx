import { useState } from 'react';

export function Random() {
  const [counter, setCounter] = useState<number>(0);

  return <div onClick={() => {
    console.log('test 123f')
    setCounter(counter + 1);
  }}>{counter * 100}</div>;
}

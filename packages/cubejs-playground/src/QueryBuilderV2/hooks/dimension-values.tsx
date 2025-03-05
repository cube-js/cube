import { CubeApi, ResultSet } from '@cubejs-client/core';
import { useEffect, useRef, useState } from 'react';

interface UseValueSuggestionsProps {
  cubeApi?: CubeApi;
  dimension?: string;
  skip?: boolean;
  mutexObj?: Record<string, any>;
}

export function useDimensionValues({
  cubeApi,
  mutexObj,
  dimension,
  skip,
}: UseValueSuggestionsProps) {
  const [isLoading, setIsLoading] = useState(false);
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const requestIdRef = useRef<string>();
  const mutexRef = useRef({});

  mutexObj = mutexObj ?? mutexRef.current;

  function loadSuggestions() {
    setError(null);

    if (!dimension || !cubeApi || dimension === requestIdRef.current) {
      return;
    }

    requestIdRef.current = dimension;

    setIsLoading(true);

    cubeApi
      .load(
        {
          dimensions: [dimension],
        },
        {
          mutexObj,
          mutexKey: `dimension-values:${dimension}`,
        }
      )
      .then((resultSet: ResultSet) => {
        if (dimension !== requestIdRef.current) {
          return; // Ignore outdated responses
        }

        // @ts-ignore
        const data = resultSet.loadResponse?.results?.[0]?.data || [];
        const dimensionValues = data.map((r: any) => r[dimension]).filter(Boolean); // Filter out undefined or null values

        if (dimensionValues.join() !== suggestions.join()) {
          setSuggestions(dimensionValues);
        }

        setIsLoading(false);
      })
      .catch((e) => {
        if (dimension !== requestIdRef.current) {
          return;
        }

        setError(String(e.message || e)); // More specific error handling
        setIsLoading(false);
        requestIdRef.current = undefined;
      });
  }

  useEffect(() => {
    mutexRef.current = {};
    requestIdRef.current = undefined;
  }, [cubeApi]);

  useEffect(() => {
    if (dimension && !skip) {
      loadSuggestions();
    } else {
      setIsLoading(false);
    }
  }, [dimension, cubeApi, skip]);

  return {
    isLoading,
    suggestions,
    error,
  };
}

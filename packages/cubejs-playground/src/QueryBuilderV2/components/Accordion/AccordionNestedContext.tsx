import { createContext, useContext } from 'react';

import { AccordionNestedContextData } from './types';

export const AccordionNestedContext = createContext<AccordionNestedContextData | null>(null);

export const useAccordionNestedContext = () => useContext(AccordionNestedContext);

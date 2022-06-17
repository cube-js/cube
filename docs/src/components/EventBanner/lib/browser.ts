import { Event } from "./api";
import {getDate} from "./date";

function checkEvents () {
  return Boolean(window?.sessionStorage?.getItem(`cubedev-event-banner_v1_${getDate()}`));
}

function readEvents (): Event[] {
  const data = window?.sessionStorage?.getItem(`cubedev-event-banner_v1_${getDate()}`);
  
  if (data === null) {
    return [];
  }

  try {
    return JSON.parse(data);
  } catch (error) {
    console.error(error);
    return [];
  }
}

function writeEvents (events: Event[]) {
  window?.sessionStorage?.setItem(`cubedev-event-banner_v1_${getDate()}`, JSON.stringify(events));
}

export {checkEvents, readEvents, writeEvents};

import classnames from 'classnames/bind';
import { addDays, format, formatDistanceToNow, isAfter, isWithinInterval } from 'date-fns';
import Image from 'next/image';
import Link from 'next/link';

import * as styles from './EventPromoBlock.module.scss';
import UpcomingIcon from './upcoming.svg';
import NowIcon from './now.svg';

const cn = classnames.bind(styles);

const EventPromoDate = ({ startsAt, endsAt }) => {
  const hasEventOccurred = isAfter(Date.now(), startsAt);
  const isEventThisWeek = isWithinInterval(startsAt, {
    start: new Date(),
    end: addDays(new Date(), 7),
  });

  // If the event has an end date, and is happening now, show "Live".
  if (endsAt) {
    const isEventHappeningNow = isWithinInterval(new Date(), {
      start: startsAt,
      end: endsAt,
    });
    if (isEventHappeningNow) {
      return (
        <div className={cn('EventPromoBlock__Date', 'EventPromoBlock__Date--Now')}>
          <Image src={NowIcon} alt='Now' />
          Live
        </div>
      );
    }
  }

  // If the event is happening this week, show "Upcoming".
  if (isEventThisWeek) {
    return (
      <div className={cn('EventPromoBlock__Date', 'EventPromoBlock__Date--Upcoming')}>
        <Image src={UpcomingIcon} alt='Upcoming' />
        in {formatDistanceToNow(startsAt)}
      </div>
    );
  }

  // If the event has already happened, show "Past Event".
  if (hasEventOccurred) {
    return (
      <div className={cn('EventPromoBlock__Date')}>
        Past Event
      </div>
    );
  }

  // If the event is in the future, show the date.
  return (
    <div className={cn('EventPromoBlock__Date')}>
      {format(startsAt, 'MMM d, yyyy')}
    </div>
  );
};

export interface CloudPromoBlockProps {
  coverUrl: string;
  startsAt: Date;
  endsAt?: Date;
  title: string;
  linkText: string;
  linkUrl: string;
}

export const EventPromoBlock = ({
  coverUrl,
  startsAt,
  endsAt,
  linkText,
  linkUrl,
  title,
}: CloudPromoBlockProps) => {
  return (
    <div className={cn('EventPromoBlock__Wrapper')}>
      <div className={cn('EventPromoBlock__Image')}>
        <img alt={title} src={coverUrl} />
      </div>
      <div className={cn('EventPromoBlock__Details')}>
        <EventPromoDate startsAt={startsAt} endsAt={endsAt} />
        <div className={cn('EventPromoBlock__Title')}>
          {title}
        </div>
        <div className={cn('EventPromoBlock__CTA')}>
          <Link href={linkUrl}>{linkText}</Link>
        </div>
      </div>
    </div>
  );
};

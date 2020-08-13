import React, { useEffect, useState } from 'react';
import {
  loadChannelsWithReactions,
  loadMembersAndJoins,
  loadMembersWithReactions,
  loadMessagesAndReactions,
  loadMessagesByWeekday,
  loadMessagesByHour,
  loadMessagesByChannel,
  loadMembersByChannel,
} from '../api';
import styles from './styles.module.css';
import MemberList from '../MemberList';
import ChannelList from '../ChannelList';
import Header from '../Header';
import Banner from '../Banner';
import MessagesChart from '../MessagesChart';
import MembersChart from '../MembersChart';
import WeekChart from '../WeekChart';
import HourChart from '../HourChart';
import MapChart from '../MapChart';
import ChannelChart from '../ChannelChart';
import PeriodAndGranularitySelector from "../PeriodAndGranularitySelector"

const defaultPeriod = 'last year';
const defaultGranularity = 'month';

const defaultListSize = 5;

function App() {
  const [period, setPeriod] = useState(defaultPeriod);
  const [granularity, setGranularity] = useState(defaultGranularity);

  function setPeriodAndGranularity(period, granularity) {
    setPeriod(period);
    setGranularity(granularity);
  }

  const [membersList, setMembersList] = useState([]);
  const [channelsList, setChannelsList] = useState([]);
  const [messages, setMessages] = useState([]);
  const [members, setMembers] = useState([]);
  const [messagesByWeekday, setMessagesByWeekday] = useState([]);
  const [messagesByHour, setMessagesByHour] = useState([]);
  const [messagesByChannel, setMessagesByChannel] = useState([]);
  const [membersByChannel, setMembersByChannel] = useState([]);

  useEffect(() => {
    loadMembersWithReactions().then(setMembersList);
    loadChannelsWithReactions().then(setChannelsList);
    loadMessagesAndReactions(period, granularity).then(setMessages);
    loadMembersAndJoins(period, granularity).then(setMembers);
    loadMessagesByWeekday(period).then(setMessagesByWeekday);
    loadMessagesByHour(period).then(setMessagesByHour);
    loadMessagesByChannel().then(setMessagesByChannel);
    loadMembersByChannel().then(setMembersByChannel);
  }, [
    period,
    granularity,
  ]);

  const [membersListDoShowAll, setMembersListDoShowAll] = useState(false);
  const [channelsListDoShowAll, setChannelsListDoShowAll] = useState(false);

  return (
    <div className={styles.root}>
      <div className={styles.content}>
        <Header />
        <div className={styles.header}>
          <h1>Activity in all channels by all members</h1>
          <PeriodAndGranularitySelector
            period={period}
            granularity={granularity}
            onSelect={setPeriodAndGranularity}
          />
        </div>
        <MessagesChart data={messages} granularity={granularity} />
        <MembersChart data={members} />
        {period !== 'last week' && <WeekChart data={messagesByWeekday} />}
        <HourChart data={messagesByHour} />
        <MapChart data={messagesByWeekday} />
        <div className={styles.row}>
          <div className={styles.column}>
            <ChannelChart
              title='Messages by channel'
              data={messagesByChannel}
            />
          </div>
          <div className={styles.column}>
            <ChannelChart title='Members by channel' data={membersByChannel} />
          </div>
        </div>
      </div>
      <div className={styles.sidebar}>
        <Banner />
        <MemberList
          data={membersList}
          limit={membersListDoShowAll ? undefined : defaultListSize}
          onShow={() => setMembersListDoShowAll(!membersListDoShowAll)}
        />
        <ChannelList
          data={channelsList}
          limit={channelsListDoShowAll ? undefined : defaultListSize}
          onShow={() => setChannelsListDoShowAll(!channelsListDoShowAll)}
        />
      </div>
    </div>
  );
}

export default App;

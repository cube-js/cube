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
import styles from './App.module.css';
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

const defaultListSize = 5;

function App() {
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
    loadMessagesAndReactions().then(setMessages);
    loadMembersAndJoins().then(setMembers);
    loadMessagesByWeekday().then(setMessagesByWeekday);
    loadMessagesByHour().then(setMessagesByHour);
    loadMessagesByChannel().then(setMessagesByChannel);
    loadMembersByChannel().then(setMembersByChannel);
  }, []);

  const [membersListDoShowAll, setMembersListDoShowAll] = useState(false);
  const [channelsListDoShowAll, setChannelsListDoShowAll] = useState(false);

  return (
    <div className={styles.root}>
      <div className={styles.content}>
        <Header />
        <div className={styles.controls}>
          <h1>All activity in all channels by all members</h1>
        </div>
        <MessagesChart data={messages} />
        <MembersChart data={members} />
        <WeekChart data={messagesByWeekday} />
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

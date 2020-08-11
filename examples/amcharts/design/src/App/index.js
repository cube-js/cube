import React, { useEffect, useState } from 'react';
import {
  loadChannelsWithReactions,
  loadMembersAndJoins,
  loadMembersWithReactions,
  loadMessagesAndReactions
} from "../api";
import styles from './App.module.css';
import MemberList from "../MemberList"
import ChannelList from "../ChannelList"
import Header from "../Header"
import Banner from "../Banner"
import MessagesChart from "../MessagesChart"
import MembersChart from "../MembersChart"

function App() {
  const [ membersList, setMembersList ] = useState([]);
  const [ channelsList, setChannelsList ] = useState([]);
  const [ messages, setMessages ] = useState([]);
  const [ members, setMembers ] = useState([]);

  useEffect(() => {
    loadMembersWithReactions().then(setMembersList);
    loadChannelsWithReactions().then(setChannelsList);
    loadMessagesAndReactions().then(setMessages);
    loadMembersAndJoins().then(setMembers);
  }, []);

  return (
    <div className={styles.root}>
      <div className={styles.content}>
        <Header />
        <div className={styles.controls}>
          <h1>All activity in all channels by all members</h1>
        </div>
        <MessagesChart data={messages} />
        <MembersChart data={members} />
        <div className={styles.block}>
          <h2>Messages by day of week</h2>
          <div>Chart here…</div>
        </div>
        <div className={styles.block}>
          <h2>Messages by hour</h2>
          <div>Chart here…</div>
        </div>
        <div className={styles.block}>
          <h2>Members by time zone</h2>
          <div>Chart here…</div>
        </div>
      </div>
      <div className={styles.sidebar}>
        <Banner />
        <MemberList data={membersList.slice(0, 10)} />
        <ChannelList data={channelsList.slice(0, 10)} />
      </div>
    </div>
  );
}

export default App;

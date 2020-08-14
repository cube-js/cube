import React, { useEffect, useState } from 'react';
import { useHotkeys } from 'react-hotkeys-hook';
import {
  loadChannelsWithReactions,
  loadMembersAndJoins,
  loadMembersWithReactions,
  loadMessagesAndReactions,
  loadMessagesByWeekday,
  loadMessagesByHour,
  // loadMessagesByChannel,
  // loadMembersByChannel,
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
import UniversalFilter from '../UniversalFilter';

const defaultPeriod = 'last 365 days';
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
  // const [messagesByChannel, setMessagesByChannel] = useState([]);
  // const [membersByChannel, setMembersByChannel] = useState([]);

  const [chosenChannel, setChosenChannel] = useState(null);
  const [chosenMember, setChosenMember] = useState(null);

  const [doShowFilter, setDoShowFilter] = useState(false);

  useHotkeys('ctrl+k, cmd+k', () => setDoShowFilter(true), { filter: () => true });
  useHotkeys('esc', () => setDoShowFilter(false), { filter: () => true });

  useEffect(() => {
    loadMembersWithReactions().then(setMembersList);
    loadChannelsWithReactions().then(setChannelsList);
    loadMessagesAndReactions(period, granularity, chosenChannel, chosenMember).then(setMessages);
    loadMembersAndJoins(period, granularity, chosenChannel, chosenMember).then(setMembers);
    loadMessagesByWeekday(period, chosenChannel, chosenMember).then(setMessagesByWeekday);
    loadMessagesByHour(period, chosenChannel, chosenMember).then(setMessagesByHour);
    // loadMessagesByChannel().then(setMessagesByChannel);
    // loadMembersByChannel().then(setMembersByChannel);
  }, [
    period,
    granularity,
    chosenChannel,
    chosenMember,
  ]);

  const [membersListDoShowAll, setMembersListDoShowAll] = useState(false);
  const [channelsListDoShowAll, setChannelsListDoShowAll] = useState(false);

  return (
    <div className={styles.root}>
      <div className={styles.content}>
        <Header onClick={() => {
          setPeriod(defaultPeriod);
          setGranularity(defaultGranularity);
          setChosenChannel(null);
          setChosenMember(null);
          setDoShowFilter(false);
        }} />
        <div className={styles.header}>
          {renderHeader(period, granularity, chosenMember, chosenChannel, () => setDoShowFilter(true))}
          {doShowFilter && <UniversalFilter
            period={period}
            granularity={granularity}
            channels={channelsList}
            members={membersList}
            channel={chosenChannel}
            member={chosenMember}
            onSelect={(period, granularity, channel, member) => {
              setPeriod(period);
              setGranularity(granularity);
              setChosenChannel(channel);
              setChosenMember(member);
              setDoShowFilter(false);
            }}
            onClose={() => setDoShowFilter(false)}
          />}
        </div>
        <MessagesChart data={messages} granularity={granularity} />
        {!chosenMember && <MembersChart data={members} />}
        {period !== 'last week' && <WeekChart data={messagesByWeekday} />}
        <HourChart data={messagesByHour} />
        <MapChart data={messagesByWeekday} />
        {/*<div className={styles.row}>*/}
        {/*  <div className={styles.column}>*/}
        {/*    <ChannelChart*/}
        {/*      title='Messages by channel'*/}
        {/*      data={messagesByChannel}*/}
        {/*    />*/}
        {/*  </div>*/}
        {/*  <div className={styles.column}>*/}
        {/*    <ChannelChart title='Members by channel' data={membersByChannel} />*/}
        {/*  </div>*/}
        {/*</div>*/}
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

function renderHeader(period, granularity, member, channel, onClick) {
  const channelPart = channel
    ? <>in <span className={styles.filtered}>#{channel}</span></>
    : <>in <span className={styles.filtered}>all channels</span></>

  const memberPart = member
    ? <>by <span className={styles.filtered}>@{member}</span></>
    : <>by <span className={styles.filtered}>all members</span></>

  const periodPart = <span className={styles.filtered}>{period}</span>

  const granularityPart = <>by <span className={styles.filtered}>{granularity}</span></>

  return (
    <h2
      className={styles.filter}
      title='Press Cmd+K or Ctrl+K to toggle filter'
      onClick={onClick}
    >
      Activity {memberPart} {channelPart} {periodPart} {granularityPart}
    </h2>
  )
}

export default App;

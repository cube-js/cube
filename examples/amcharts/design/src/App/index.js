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
import { AutoComplete, DatePicker, Dropdown } from 'antd';
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

const { RangePicker } = DatePicker;

function App() {
  const [membersList, setMembersList] = useState([]);
  const [channelsList, setChannelsList] = useState([]);
  const [messages, setMessages] = useState([]);
  const [members, setMembers] = useState([]);
  const [messagesByWeekday, setMessagesByWeekday] = useState([]);
  const [messagesByHour, setMessagesByHour] = useState([]);
  const [messagesByChannel, setMessagesByChannel] = useState([]);
  const [membersByChannel, setMembersByChannel] = useState([]);

  const [chosenUser, setChosenUser] = useState(null);
  const [chosenChannel, setChosenChannel] = useState(null);

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

  useEffect(() => {
    loadMessagesAndReactions(chosenChannel).then(setMessages);
  }, [chosenChannel]);

  const onDateChange = (e) => {
    e.preventDefault();
    e.stopPropagation();
    return false;
  };

  return (
    <div className={styles.root}>
      <div className={styles.content}>
        <Header />
        <div className={styles.controls}>
          <h1>
            All activity in&nbsp;
            <Dropdown
              placement='bottomCenter'
              overlay={
                <div className={styles.dropdown}>
                  <div
                    className={[
                      styles.dropdownItem,
                      styles.dropdownItemChannel,
                    ]}
                    onClick={() => {
                      setChosenChannel(null);
                    }}
                  >
                    all channels
                  </div>
                  {channelsList.map((channel) => (
                    <div
                      className={styles.dropdownItem}
                      onClick={() => {
                        setChosenChannel(channel.name);
                      }}
                    >
                      {channel.name}
                    </div>
                  ))}
                </div>
              }
            >
              <span>
                {chosenChannel ? `#${chosenChannel}` : 'all channels'}
              </span>
            </Dropdown>
            &nbsp;by&nbsp;
            <Dropdown
              placement='bottomCenter'
              overlay={
                <div className={styles.dropdown}>
                  <div
                    className={styles.dropdownItem}
                    onClick={() => {
                      setChosenUser(null);
                    }}
                  >
                    all users
                  </div>
                  {membersList.map((member) => (
                    <div
                      className={styles.dropdownItem}
                      onClick={() => {
                        setChosenUser(member.name);
                      }}
                    >
                      {member.name}
                    </div>
                  ))}
                </div>
              }
            >
              <span>{chosenUser ? `@${chosenUser}` : 'all users'}</span>
            </Dropdown>
            &nbsp;at&nbsp;
            <Dropdown
              placement='bottomCenter'
              overlay={
                <div className={styles.dropdown}>
                  <div className={styles.dropdownItem}>all time</div>
                  <div className={styles.dropdownItem}>Last 30 days</div>
                  <div className={styles.dropdownItem}>
                    <RangePicker
                      format='YYYY-MM-DD'
                      onChange={onDateChange}
                      onOk={onDateChange}
                    />
                  </div>
                </div>
              }
            >
              <span>all time</span>
            </Dropdown>
          </h1>
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
        <MemberList data={membersList.slice(0, 10)} />
        <ChannelList data={channelsList.slice(0, 10)} />
      </div>
    </div>
  );
}

export default App;

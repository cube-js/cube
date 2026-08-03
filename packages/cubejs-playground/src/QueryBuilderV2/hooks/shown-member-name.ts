import { titleize } from '../utils';
import { MemberViewType } from '../types';

export interface UseMemberShownNameProps {
  cubeName: string;
  cubeTitle?: string;
  memberName: string;
  memberTitle?: string;
  type?: MemberViewType;
}

export function useShownMemberName(props: UseMemberShownNameProps) {
  let { cubeName, cubeTitle, memberName, memberTitle, type = 'name' } = props;

  memberName = memberName.split('.')[1] ?? memberName;

  const shownCubeName = type === 'name' ? cubeName : (cubeTitle ?? titleize(cubeName));
  const shownMemberName = type === 'name' ? memberName : (memberTitle ?? titleize(memberName));
  const shownFullName = `${shownCubeName}${type === 'name' ? '.' : ' '}${shownMemberName}`;
  const isAutoCubeName = cubeTitle && cubeName === titleize(cubeTitle);
  const isAutoMemberName = memberTitle && memberName === titleize(memberTitle);
  const isAutoName = isAutoCubeName && isAutoMemberName;

  return {
    shownCubeName,
    shownMemberName,
    shownFullName,
    isAutoName,
    isAutoMemberName,
    isAutoCubeName,
  };
}

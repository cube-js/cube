import styled from 'styled-components';

const SectionRow = styled.div`
  display: flex;
  flex-flow: row; 
  margin-right: -8px;
  margin-bottom: -8px;
  
  > * {
    margin-right: 8px;
    margin-bottom: 8px;
  }
`;

export default SectionRow;

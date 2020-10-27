import styled from 'styled-components';

const SectionRow = styled.div`
  display: flex;
  flex-flow: row wrap; 
  margin-right: -8px;
  margin-bottom: -8px;
  
  && > * {
    margin-right: 8px !important;
    margin-bottom: 8px !important;
  }
`;

export default SectionRow;

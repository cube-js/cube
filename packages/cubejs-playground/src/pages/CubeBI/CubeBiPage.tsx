import { Button, Typography } from 'antd';
import styled from 'styled-components';
import { Header } from '../components/Ui';

const { Title, Paragraph, Link } = Typography;

const CenteredContainer = styled.div`
  display: flex;
  flex-direction: column;
  align-items: center;
  text-align: center;
  margin-top: 48px;
`;

const StyledHeader = styled(Header)`
  width: 100%;
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 24px 50px;

  h1 {
    margin: 0;
  }
`;

const VideoContainer = styled.div`
  margin-top: 48px;
  width: 100%;
  max-width: 800px;
  aspect-ratio: 16 / 9;
  
  iframe {
    width: 100%;
    height: 100%;
    border: none;
    border-radius: 8px;
  }
`;

export function CubeBiPage() {
  return (
    <CenteredContainer>
      <StyledHeader>
        <Title>Cube — Modern BI Tool from Cube Core Creators</Title>
        <Paragraph style={{ fontSize: '18px', marginTop: '16px', color: '#666' }}>
          <Link href="https://cube.dev?ref=github-readme" target="_blank">Cube</Link> is an agentic analytics platform built on Cube Core. It provides a user-friendly interface with reporting and dashboard capabilities, leveraging the Cube Core data model.
        </Paragraph>
        <Button
          type="primary"
          size="large"
          style={{ marginTop: '24px' }}
          href="https://cubecloud.dev/auth/signup"
          target="_blank"
        >
          Get Started for Free
        </Button>
      </StyledHeader>
      <VideoContainer>
        <iframe
          src="https://www.youtube.com/embed/n-U1FvHqE2Q?start=59"
          title="Cube BI Video"
          allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
          allowFullScreen
        />
      </VideoContainer>
    </CenteredContainer>
  );
}


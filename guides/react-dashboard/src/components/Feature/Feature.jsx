import React from "react";
import styled from 'styled-components';
import media from "styled-media-query";
import theme from '../../theme';

const Container = styled.div`
  max-width: ${theme.landingMaxWidth};
  padding: 0 ${theme.landingPadding};
  ${media.lessThan("large")`
    padding-left: ${theme.landingPaddingLessThenLarge};
    padding-right: ${theme.landingPaddingLessThenLarge};
  `}
  margin: 100px auto 0 auto;
  display: flex;
  justify-content: space-between;
  align-items: center;
  ${media.lessThan("medium")`
    flex-direction: ${props => props.rightAligned ? "column-reverse" : "column"};
    text-align: center;
  `}
`

const Image = styled.img`
  max-width: 540px;
  ${media.lessThan("medium")`
    width: 90%;
  `}
`

const CopyContainer = styled.div`
  max-width: 600px;
`

const MetaTitle = styled.div`
  color: ${theme.colors.grey};
  letter-spacing: 0.02em;
  font-size: 16px;
  text-transform: uppercase;
  margin-bottom: 30px;
`

const Title = styled.div`
  margin-bottom: 20px;
  color: ${theme.colors.darkPurple};
  font-size: 26px;
  line-height: 36px;
  font-weight: bold;
`

const Text = styled.div`
  color: ${theme.colors.grey};
  line-height: 30px;
  font-size: 20px;
`

const Feature = ({
  imageAlign,
  image,
  metaTitle,
  title,
  text
}) => (
  <Container rightAligned={imageAlign === 'right'}>
    { imageAlign === 'left' &&
    <div>
       <Image src={image} />
     </div>
    }
    <CopyContainer>
       <MetaTitle>{ metaTitle }</MetaTitle>
       <Title>{ title }</Title>
       <Text>{ text }</Text>
    </CopyContainer>
    { imageAlign === 'right' &&
    <div>
       <Image src={image} />
     </div>
    }
  </Container>
);

export default Feature;

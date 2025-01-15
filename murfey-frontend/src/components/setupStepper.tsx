import {
  Box,
  Step,
  StepDescription,
  StepIcon,
  StepIndicator,
  StepNumber,
  StepSeparator,
  StepStatus,
  StepTitle,
  Stepper,
  useSteps,
} from "@chakra-ui/react";

import React from "react";

const getUrl = (endpoint: string) => {
  return (
    (sessionStorage.getItem("murfeyServerURL") ??
      process.env.REACT_APP_API_ENDPOINT) + endpoint
  );
};

interface StepperStartConditions {
  activeStepIndex: number;
}

const SetupStepper = ({ activeStepIndex }: StepperStartConditions) => {
  const steps = [
    { title: "Visit", description: "Select visit" },
    { title: "Gain reference", description: "Transfer and transform" },
    { title: "Parameters", description: "For processing" },
    { title: "Confirm", description: "Start data transfer" },
  ];

  const { activeStep } = useSteps({
    index: activeStepIndex,
    count: steps.length,
  });

  return (
    <Stepper index={activeStep}>
      {steps.map((step, index) => (
        <Step key={index}>
          <StepIndicator>
            <StepStatus
              complete={<StepIcon />}
              incomplete={<StepNumber />}
              active={<StepNumber />}
            />
          </StepIndicator>

          <Box flexShrink="0">
            <StepTitle>{step.title}</StepTitle>
            <StepDescription>{step.description}</StepDescription>
          </Box>

          <StepSeparator />
        </Step>
      ))}
    </Stepper>
  );
};

export { SetupStepper };

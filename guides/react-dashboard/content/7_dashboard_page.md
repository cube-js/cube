---
title: "Dashboard Page"
order: 7
---

This is going to be a small section. We have already created some components, while customizing the Explore page, which we are going to reuse here. The image below shows the final look of the Dashboard page after we finish styling it.

<IMAGE>

First, we are going to add the `<PageHeader />` component to the Dashboard page. We’ve already created it for the Explore page, so let’s reuse it here.

Make the following changes to the `src/pages/DashboardPage.js` file.

```diff
 import React from "react";
-import { Spin, Button, Alert } from "antd";
+import { Spin, Button, Alert, Typography } from "antd";
 import { Link } from "react-router-dom";
 import { useQuery } from "@apollo/react-hooks";
 import { GET_DASHBOARD_ITEMS } from "../graphql/queries";
 import ChartRenderer from "../components/ChartRenderer";
 import Dashboard from "../components/Dashboard";
 import DashboardItem from "../components/DashboardItem";
+import PageHeader from "../components/PageHeader";
```

```diff
   return !data || data.listDashboardItems.items.length ? (
-    <Dashboard dashboardItems={data && data.listDashboardItems.items}>
-      {data && data.listDashboardItems.items.map(deserializeItem).map(dashboardItem)}
-    </Dashboard>
+    <div>
+      <PageHeader
+        title={<Typography.Title level={4}>Dashboard</Typography.Title>}
+        button={<Link to="/explore">
+          <Button type="primary">
+            Add chart
+          </Button>
+        </Link>}
+      />
+      <Dashboard dashboardItems={data && data.listDashboardItems.items}>
+        {data && data.listDashboardItems.items.map(deserializeItem).map(dashboardItem)}
+      </Dashboard>
+    </div>
   ) : <Empty />;
```

Now, we need to make some small changes to the layout of the dashboard itself in the `<Dashboard />` component and the look of the dashboard item in the `<DashboardItem />` component.

 Make the following changes in `src/components/Dashboard.js`.

```diff
-<ReactGridLayout cols={12} rowHeight={50} onLayoutChange={onLayoutChange}>
+<ReactGridLayout
+  style={{marginLeft: 18, marginRight: 18, marginTop: 6}}
+  cols={12}
+  rowHeight={50}
+  onLayoutChange={onLayoutChange}
+ >
```

Let’s update the styles of the  `<DashboardItem />` with Styled Components and also change the icon for the dropdown menu. Update `src/components/DashboardItem.js` as shown below.

```diff
 import React from "react";
-import { Card, Menu, Button, Dropdown, Modal } from "antd";
+import { Card, Menu, Icon, Dropdown, Modal } from "antd";
+import styled from 'styled-components';
 import { useMutation } from "@apollo/react-hooks";
 import { Link } from "react-router-dom";
 import { GET_DASHBOARD_ITEMS } from "../graphql/queries";
 import { DELETE_DASHBOARD_ITEM } from "../graphql/mutations";

+const StyledCard = styled(Card)`
+  box-shadow: 0px 2px 4px rgba(141, 149, 166, 0.1);
+  border-radius: 4px;
+
+  .ant-card-head {
+    border: none;
+  }
+  .ant-card-body {
+    padding-top: 12px;
+  }
+`
+
```

Update the icon for the dropdown menu.

```diff
  <Dropdown
    overlay={dashboardItemDropdownMenu}
    placement="bottomLeft"
    trigger={["click"]}
  >
-   <Button shape="circle" icon="menu" />
+   <Icon type="menu" />
  </Dropdown>
```

And finally, use `<StyledCard />` from Styled Components to display the chart’s container.

```diff
-  <Card
+  <StyledCard
     title={title}
+    bordered={false}
     style={{
       height: "100%",
       width: "100%"
     }}
     extra={<DashboardItemDropdown itemId={itemId} />}
   >
     {children}
-  </Card>
+  </StyledCard>
```

That was the last part on customization of our React dashboard application. So
far we've customized both Explore and the Dashboard pages, as well as the query
builder and the charts.

I hope you learned a lot on how to build a custom analytics app, which can either
be used internally or embedded into existing applications. If you have
questions, feel free to ask them in this [Slack community](https://slack.cube.dev).

In the next and final part, we'll learn how to deploy our application.

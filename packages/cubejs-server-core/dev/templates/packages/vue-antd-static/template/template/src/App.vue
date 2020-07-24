<template>
    <div>
        <a-layout-header class="header">

            <h2 style="color: rgb(255, 255, 255); margin: 0px 1em 0px 0px; display: inline; width: 200px; line-height: 54px;float: left">
                My Dashboard</h2>

          <a-menu
                  theme="dark"
                  mode="horizontal"
                  :default-selected-keys="['1']"
                  style="float: left"
                  :style="{ lineHeight: '64px' }"
          >
            <a-menu-item key="1" @click="key=1">
              ExplorePage
            </a-menu-item>
            <a-menu-item key="2" @click="key=2">
              DashboardPage
            </a-menu-item>

          </a-menu>


        </a-layout-header>


      <ExplorePage v-show="key==1"></ExplorePage>
      <DashboardPage v-show="key==2"></DashboardPage>


    </div>
</template>
<script>

    import DashboardPage from "~/components/DashboardPage";
    import ExplorePage from "~/components/ExplorePage";

    export default {
        components: {ExplorePage, DashboardPage},
        data() {
            return {
              key:1,
            };
        },
        methods: {
            callback(key) {
                console.log(key);
            },
        },
    };
</script>

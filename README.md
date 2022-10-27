# 111-Advanced-Data-Mining-and-Big-Data-Analysis

- Lab1:優化Hadoop Average.java程式
- Lab2:設計Hadoop版本之 K-Means Algorithm Implementation
- Lab3:Following Behaviors in Ep.txt

---

## 指令
- 登入指令
    - `ssh kang@140.120.182.143 -p 1000`
- 編譯
    - `ant`
- 執行
    - `hadoop jar output.jar FollowingBehaviors Ep@HDFS EpFollowingBehaviorsResult1 EpFollowingBehaviorsResult2`
- 查看執行結果
    - `hadoop fs -cat EpFollowingBehaviorsResult2/part-r-00000`

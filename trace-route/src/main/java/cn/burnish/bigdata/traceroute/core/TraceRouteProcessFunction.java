package cn.burnish.bigdata.traceroute.core;

import cn.burnish.bigdata.traceroute.datatypes.Segment;
import cn.burnish.bigdata.traceroute.datatypes.TraceRouteResult;
import cn.burnish.common.datatypes.CapConfigBroadcastData;
import cn.burnish.common.datatypes.SeriesStandData;
import cn.burnish.common.datatypes.TruckData;
import lombok.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class TraceRouteProcessFunction extends KeyedBroadcastProcessFunction<String, Segment, CapConfigBroadcastData, TraceRouteResult> {
    MapStateDescriptor<Void, CapConfigBroadcastData> traceRouteBroadcastDescriptor = new MapStateDescriptor<>("traceRouteBroadcast", Void.class, CapConfigBroadcastData.class);
    private transient ValueState<TraceRouteResult> output; // 每次更新的数据
    private transient ValueState<Tuple2<Double, Long>> preMileage; // 上次有效里程
    private transient ValueState<Tuple2<Double, Long>> prePower; // 上次有效电量
    private transient ValueState<Tuple2<Double, Long>> preOil; // 上次有效油量
    private transient ValueState<Tuple2<Double, Long>> preUrea; // 上次有效尿素
    private transient ValueState<Tuple2<Double, Long>> preSpeed; // 上次速度
    private transient ValueState<Long> timer; // 定时器时间

    public static long FIFTEEN_MINUTE = 1000 * 60 * 15;
    //    public static long FIFTEEN_MINUTE = 1000 * 15;
    public static Logger logger = LoggerFactory.getLogger(Process.class);


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        output = getRuntimeContext().getState(new ValueStateDescriptor<>("outData", TraceRouteResult.class));
        preMileage = getRuntimeContext().getState(new ValueStateDescriptor<>("preMileage", Types.TUPLE(Types.DOUBLE, Types.LONG)));
        prePower = getRuntimeContext().getState(new ValueStateDescriptor<>("prePower", Types.TUPLE(Types.DOUBLE, Types.LONG)));
        preOil = getRuntimeContext().getState(new ValueStateDescriptor<>("preOil", Types.TUPLE(Types.DOUBLE, Types.LONG)));
        preUrea = getRuntimeContext().getState(new ValueStateDescriptor<>("preUrea", Types.TUPLE(Types.DOUBLE, Types.LONG)));
        preSpeed = getRuntimeContext().getState(new ValueStateDescriptor<>("preSpeed", Types.TUPLE(Types.DOUBLE, Types.LONG)));
        timer = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Types.LONG));
    }


    public void clear() throws IOException {
        // 开始新的一段统计
        TraceRouteResult outputValue = output.value();
        outputValue.stopObdTime = null;
        outputValue.mileage = 0.;
        outputValue.oil = 0.;
        outputValue.power = 0.;
        outputValue.powerKwh = 0.;
        outputValue.urea = 0.;
        outputValue.maxSpeed = 0.;
        outputValue.delayTime = 0L;
        output.update(outputValue);
        preMileage.clear();
        prePower.clear();
        preOil.clear();
        preUrea.clear();
        preSpeed.clear();
    }


    /**
     * 处理最大速度
     */
    public void dealSpeed(Segment value, TraceRouteResult outputValue) throws IOException {
        if (value.speed == null) return;

        if (value.speed > outputValue.maxSpeed) {
            outputValue.maxSpeed = value.speed;
        }
        preSpeed.update(Tuple2.of(value.speed, value.obdTime));
    }


    /**
     * 处理里程
     */
    public void dealMileage(Segment value, TraceRouteResult outputData) throws IOException {
        // 过滤掉里程为空的数据，以及里程为0的数据
        if (value.mileage == null || value.mileage == 0. || value.mileage > 1000000) return;
        if (preMileage.value() != null) {
            double diffMileage = value.mileage - preMileage.value().f0;
            if (diffMileage > 0 && diffMileage < 30) {
                outputData.mileage += diffMileage;
            }
        }
        preMileage.update(Tuple2.of(value.mileage, value.obdTime));
    }

    /**
     * 处理油耗
     */
    public void dealOil(Segment value, CapConfigBroadcastData broadcastMap, TraceRouteResult outputValue) throws IOException {
        // 过滤掉里程为空的数据
        if (value.oil == null || value.oil == 0) return;

        // 过滤油量大于邮箱配置的数据
        SeriesStandData seriesStandData = broadcastMap.seriesStandMap.get(value.series);
        if (seriesStandData != null && value.oil > seriesStandData.maxOil) return;

        if (preOil.value() != null) {
            double diffOil = value.oil - preOil.value().f0;
            if (diffOil < 0) {
                outputValue.oil -= diffOil;
            }
        }
        preOil.update(Tuple2.of(value.oil, value.obdTime));
    }

    /**
     * 处理电耗
     */
    public void dealPower(Segment value, TraceRouteResult outputValue) throws IOException {
        // 过滤掉里程为空的数据
        if (value.power == null || value.power == 0) return;
        if (prePower.value() != null) {
            double diffPower = value.power - prePower.value().f0;
            if (diffPower < 0) {
                outputValue.power -= diffPower;
            }
        }
        prePower.update(Tuple2.of(value.power, value.obdTime));
    }

    /**
     * 处理尿耗
     */
    public void dealUrea(Segment value, CapConfigBroadcastData dailyMapInfoData, TraceRouteResult outputValue) throws IOException {
        // 过滤掉里程为空的数据
        if (value.urea == null || value.urea == 0) return;

        // 过滤掉尿素大于配置的数据
        SeriesStandData seriesStandData = dailyMapInfoData.seriesStandMap.get(value.series);
        if (seriesStandData != null && value.urea > seriesStandData.maxUrea) return;

        if (preUrea.value() != null) {
            double diffUrea = value.urea - preUrea.value().f0;
            if (diffUrea < 0) {
                outputValue.urea -= diffUrea;
            }
        }
        preUrea.update(Tuple2.of(value.urea, value.obdTime));
    }

    /**
     * 处理怠速时长
     */
    public void dealDelayTime(Segment value, TraceRouteResult outputValue) throws IOException {
        // 当前车速和上一次车速，均为0才会累积怠速时长
        if (value.speed != null && value.speed == 0 && preSpeed.value() != null && preSpeed.value().f0 == 0) {
            long diffTime = value.obdTime - preSpeed.value().f1;
            outputValue.delayTime += diffTime;
        }
    }

    public void dealStatistic(Segment value, CapConfigBroadcastData broadcastMap) throws IOException {
        TraceRouteResult outputValue = output.value();
        outputValue.endObdTime = value.obdTime;
//        if (output.value().endObdTime == null) {
//            logger.info("endObdTime, 执行了计算但还是为null : value.obdTime的值是" + value.obdTime + " output.value().endObdTime的值是" + output.value().endObdTime + "outputValue.endObdTime是" + outputValue.obdTime);
//        }
        outputValue.obdTime = value.obdTime;
        outputValue.endLng = value.lng;
        outputValue.endLat = value.lat;
        dealDelayTime(value, outputValue);  // speed 必须放在这个之后
        dealMileage(value, outputValue);
//        dealOil(value, broadcastMap, outputValue);
//        dealUrea(value, broadcastMap, outputValue);
//        dealPower(value, outputValue);
        dealSpeed(value, outputValue);
        output.update(outputValue);
    }

    public void updateTimer() throws IOException {
        timer.update(System.currentTimeMillis() + FIFTEEN_MINUTE);
    }


@Override
public void processElement(Segment value, KeyedBroadcastProcessFunction<String, Segment, CapConfigBroadcastData, TraceRouteResult>.ReadOnlyContext ctx, Collector<TraceRouteResult> out) throws Exception {
        if (output.value() != null && value.obdTime < output.value().obdTime) return;  // 乱序到达
        // 广播数据
        ReadOnlyBroadcastState<Void, CapConfigBroadcastData> broadcastState = ctx.getBroadcastState(traceRouteBroadcastDescriptor);
        CapConfigBroadcastData broadcastMap = broadcastState.get(null);
        if (broadcastMap == null) return;  // 如果配置文件没有加载完成
        if (!broadcastMap.truckMap.containsKey(value.vin)) return; // 不是车队的vin
        // 第一次初始化
        TruckData truckData = broadcastMap.truckMap.get(value.vin);
        if (output.value() == null) {
//            TruckData truckData = broadcastMap.truckMap.get(value.vin);
            updateTimer();
            ctx.timerService().registerProcessingTimeTimer(timer.value());
            TraceRouteResult r = new TraceRouteResult(
                    value.vin,
                    value.obdTime,
                    value.lng,
                    value.lat,
                    null,
                    null,
                    null,
                    null,
                    truckData.Company,
                    truckData.Team,
                    truckData.ID,
                    0.,
                    0.,
                    0.,
                    0.,
                    0.,
                    0.,
                    0L,
                    value.obdTime
            );
            output.update(r);
            dealStatistic(value, broadcastMap);
        } else {
            TraceRouteResult outputValue = output.value();
            // 超过15分钟未发送数据后发送来新的数据，视为新的行程
            if (outputValue.obdTime + FIFTEEN_MINUTE < value.obdTime) {
                outputValue.stopObdTime = value.obdTime;
                out.collect(outputValue);
                // 数据落盘后再更新车辆的数据
                outputValue.companyId = truckData.Company;
                outputValue.teamId = truckData.Team;
                outputValue.truckId = truckData.ID;
                outputValue.startObdTime = value.obdTime;
                outputValue.startLng = value.lng;
                outputValue.startLat = value.lat;
                output.update(outputValue);  //生效了的
                clear();
            }
            // 未超过15分钟，重新设置定时器
            // 不管怎么说来了新的数据都会重新设置定时器
            ctx.timerService().deleteProcessingTimeTimer(timer.value());
            updateTimer();
            ctx.timerService().registerProcessingTimeTimer(timer.value());
            dealStatistic(value, broadcastMap);
        }
    }


    public void processBroadcastElement(CapConfigBroadcastData value, KeyedBroadcastProcessFunction<String, Segment, CapConfigBroadcastData, TraceRouteResult>.Context context, Collector<TraceRouteResult> collector) throws Exception {
        //获取状态
        BroadcastState<Void, CapConfigBroadcastData> broadcastState = context.getBroadcastState(traceRouteBroadcastDescriptor);
        //清空状态
        broadcastState.clear();
        //更新状态
        broadcastState.put(null, value);
        System.out.println("加载配置");
    }

    @Override
    public void onTimer(long timestamp, KeyedBroadcastProcessFunction<String, Segment, CapConfigBroadcastData, TraceRouteResult>.OnTimerContext ctx, Collector<TraceRouteResult> collector) throws Exception {
        TraceRouteResult outputValue = output.value();
        ReadOnlyBroadcastState<Void, CapConfigBroadcastData> broadcastState = ctx.getBroadcastState(traceRouteBroadcastDescriptor);
        CapConfigBroadcastData broadcastMap = broadcastState.get(null);
        if (broadcastMap == null) return;  // 如果配置文件没有加载完成
        if (!broadcastMap.truckMap.containsKey(outputValue.vin)) return; // 不是车队的vin
        // 第一次初始化
        TruckData truckData = broadcastMap.truckMap.get(outputValue.vin);
        outputValue.companyId = truckData.Company;
        outputValue.teamId = truckData.Team;
        outputValue.truckId = truckData.ID;

        outputValue.stopObdTime = timer.value();
        updateTimer();
        ctx.timerService().registerProcessingTimeTimer(timer.value());
        collector.collect(outputValue);
    }
}


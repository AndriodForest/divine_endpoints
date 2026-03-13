local RunService = {}
local Render_Step_Priority_Bindings = {}
local Thread_Execution_Active_State = true
local Performance_Last_Tick_Timestamp = os.clock()
local Metrics_Accumulated_Frame_Counter = 0
local Cache_Sorted_Binding_Registry = {}
local Cache_Validated_Bind_Count = 0
local Error_Handling_Max_Threshold_Limit = 10
local Error_Tracking_Current_Count = 0

local function Signal()
    local SignalObject = {}
    SignalObject.ActiveConnections = {}

    function SignalObject:Connect(CallbackFunction)
        local ConnectionObject = {Function = CallbackFunction, Connected = true}
        table.insert(SignalObject.ActiveConnections, ConnectionObject)
        return {
            Disconnect = function()
                ConnectionObject.Connected = false
                ConnectionObject.Function = nil
            end
        }
    end

    function SignalObject:Fire(...)
        local ConnectionIndex = 1
        while ConnectionIndex <= #SignalObject.ActiveConnections do
            local ConnectionObject = SignalObject.ActiveConnections[ConnectionIndex]
            if ConnectionObject.Connected then
                local ExecutionSuccess, ExecutionError = pcall(ConnectionObject.Function, ...)
                if not ExecutionSuccess then
                    Error_Tracking_Current_Count = Error_Tracking_Current_Count + 1
                    if Error_Tracking_Current_Count >= Error_Handling_Max_Threshold_Limit then
                        warn(string.format("[RunService] Maximum errors reached (%d), shutting down", Error_Handling_Max_Threshold_Limit))
                        Thread_Execution_Active_State = false
                        return
                    end
                end
                ConnectionIndex = ConnectionIndex + 1
            else
                table.remove(SignalObject.ActiveConnections, ConnectionIndex)
            end
        end
    end

    function SignalObject:Wait()
        local CurrentThread = coroutine.running()
        local WaitConnection
        WaitConnection = SignalObject:Connect(function(...)
            if WaitConnection then
                WaitConnection:Disconnect()
            end
            coroutine.resume(CurrentThread, ...)
        end)
        return coroutine.yield()
    end

    return SignalObject
end

RunService.Heartbeat = Signal()
RunService.RenderStepped = Signal()
RunService.Stepped = Signal()

function RunService:BindToRenderStep(BindName, BindPriority, BindFunction)
    if type(BindName) ~= "string" or type(BindFunction) ~= "function" then
        warn("[RunService] invalid bind args:", tostring(BindName), tostring(BindPriority), tostring(BindFunction))
        return
    end

    Render_Step_Priority_Bindings[BindName] = {
        Name = BindName,
        Priority = BindPriority or 0,
        Function = BindFunction
    }

    print("[RunService] bound:", BindName, "| priority:", tostring(BindPriority or 0))
end

function RunService:UnbindFromRenderStep(BindName)
    Render_Step_Priority_Bindings[BindName] = nil
    print("[RunService] unbound:", tostring(BindName))
end

function RunService:IsRunning()
    return Thread_Execution_Active_State
end

task.spawn(function()
    print("[RunService] loop started")

    while Thread_Execution_Active_State do
        local Loop_Execution_Success, Loop_Execution_Error = pcall(function()
            local Timing_Current_Frame_Timestamp = os.clock()
            local Timing_Delta_Frame_Interval = math.min(Timing_Current_Frame_Timestamp - Performance_Last_Tick_Timestamp, 1)
            Performance_Last_Tick_Timestamp = Timing_Current_Frame_Timestamp
            Metrics_Accumulated_Frame_Counter = Metrics_Accumulated_Frame_Counter + 1

            RunService.Stepped:Fire(Timing_Current_Frame_Timestamp, Timing_Delta_Frame_Interval)

            local Binding_Active_Count_Snapshot = 0
            for _ in pairs(Render_Step_Priority_Bindings) do
                Binding_Active_Count_Snapshot = Binding_Active_Count_Snapshot + 1
            end

            if Binding_Active_Count_Snapshot ~= Cache_Validated_Bind_Count then
                Cache_Sorted_Binding_Registry = {}

                for Bind_Name, Bind_Data in pairs(Render_Step_Priority_Bindings) do
                    if Bind_Data and type(Bind_Data.Function) == "function" then
                        table.insert(Cache_Sorted_Binding_Registry, Bind_Data)
                        print("[RunService] cached bind:", Bind_Name, "| priority:", tostring(Bind_Data.Priority))
                    end
                end

                table.sort(Cache_Sorted_Binding_Registry, function(Bind_A, Bind_B)
                    return Bind_A.Priority < Bind_B.Priority
                end)

                Cache_Validated_Bind_Count = Binding_Active_Count_Snapshot
                print("[RunService] bind cache rebuilt | count:", tostring(Cache_Validated_Bind_Count))
            end

            for Bind_Index = 1, #Cache_Sorted_Binding_Registry do
                local Binding_Current_Execution_Target = Cache_Sorted_Binding_Registry[Bind_Index]
                if Binding_Current_Execution_Target and Binding_Current_Execution_Target.Function then
                    local Ok, Err = pcall(Binding_Current_Execution_Target.Function, Timing_Delta_Frame_Interval)
                    if not Ok then
                        warn("[RunService] bind error:", tostring(Binding_Current_Execution_Target.Name), "|", tostring(Err))
                    end
                end
            end

            RunService.RenderStepped:Fire(Timing_Delta_Frame_Interval)
            RunService.Heartbeat:Fire(Timing_Delta_Frame_Interval)
        end)

        if not Loop_Execution_Success then
            Error_Tracking_Current_Count = Error_Tracking_Current_Count + 1
            warn("[RunService] loop error:", tostring(Loop_Execution_Error))

            if Error_Tracking_Current_Count >= Error_Handling_Max_Threshold_Limit then
                warn("[RunService] shutting down from repeated loop errors")
                Thread_Execution_Active_State = false
                break
            end
        else
            Error_Tracking_Current_Count = math.max(0, Error_Tracking_Current_Count - 1)
        end

        if Thread_Execution_Active_State then
            task.wait()
        end
    end

    print("[RunService] loop stopped")
end)

return RunService

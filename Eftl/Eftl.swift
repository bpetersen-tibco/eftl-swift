//
//  Eftl.swift
//  Eftl
//
//  Created by Brent Petersen on 4/30/19.
//  Copyright © 2019 TIBCO Software. All rights reserved.
//

import Foundation
import Starscream

public typealias JsonType = [String: Any]

/// Eftl delegate notified of connection events.
public protocol EftlDelegate: class {
    
    /// Called when a connection to the server is established.
    ///
    /// - Parameter eftl: The Eftl object.
    func eftlDidConnect(_ eftl: Eftl)
    
    /// Called when a connection to the server is disconnected, or cannot be established,
    /// and all reconnect attempts have been exhausted.
    ///
    /// - Parameters:
    ///   - eftl: The Eftl object.
    ///   - error: The error causing the disconnect, or `nil` if this was an intentional disconnect.
    func eftlDidDisconnect(_ eftl: Eftl, withError error: Error?)
    
    /// Called when a connection to the server is disconnected, or cannot be established,
    /// and an automatic reconnect is being attempted.
    ///
    /// Calling either `connect` or `disconnect` will immediately stop automatic reconnects.
    ///
    /// - Parameters:
    ///   - eftl: The Eftl object.
    ///   - error: The error causing the reconnect.
    ///   - delay: The time delay before the reconnect is attempted.
    func eftlWillReconnect(_ eftl: Eftl, withError error: Error?, afterDelay delay: TimeInterval)
}

/// Eftl connection options.
open class EftlOptions {
    
    /// Username used during authentication.
    public let username: String?
    
    /// Password used during authentication.
    public let password: String?
    
    /// Maximum number of reconnect attempts. A value of `-1` will retry forever.
    public let reconnectMaxAttempts: Int
    
    /// Maximum interval between retry attempts. An exponential backoff strategy is utilized.
    public let reconnectMaxInterval: TimeInterval
    
    /// Set the optional trusted server certificates.
    public let sslTrustedCertificates: [Data]?
    
    /// Set this when testing with self signed certificates.
    public let sslAllowUntrustedCertificates: Bool
    
    /// The device token with which to receive remote notifications from APNs.
    public let deviceToken: String?
    
    /// Initialize Eftl options.
    public init(username: String? = nil, password: String? = nil,
                reconnectMaxAttempts: Int = 5, reconnectMaxInterval: TimeInterval = 30,
                sslTrustedCertificates: [Data]? = nil, sslAllowUntrustedCertificates: Bool = false,
                deviceToken: String? = nil) {
        self.username = username
        self.password = password
        self.reconnectMaxAttempts = reconnectMaxAttempts
        self.reconnectMaxInterval = reconnectMaxInterval
        self.sslTrustedCertificates = sslTrustedCertificates
        self.sslAllowUntrustedCertificates = sslAllowUntrustedCertificates
        self.deviceToken = deviceToken
    }
}

/// Eftl object representing a connection to the server.
open class Eftl {
    
    // MARK: Properties
    
    /// Eftl client version.
    static public let version = "6.2.0"
    
    /// Eftl delegate.
    open weak var delegate: EftlDelegate?
    
    /// The connection state.
    open var isConnected: Bool {
        return dispatchQueue.sync {
            return state != .disconnected
        }
    }
    
    // MARK: Initializations
    
    /// Initialize Eftl.
    ///
    /// - Parameters:
    ///   - url: Server URL of the form ws[s]://[username:password@]host[:port]/channel
    ///   - clientID: A unique client identifier.
    ///   - options: Eftl options.
    ///   - delegate: Eftl delegate to be notified of connection events.
    public init(url: String, clientID: String?, options: EftlOptions = EftlOptions(), withDelegate delegate: EftlDelegate?) {
        self.urlList = url.components(separatedBy: "|").shuffled()
        self.clientID = clientID
        self.options = options
        self.delegate = delegate
    }
    
    /// Initialize Eftl.
    ///
    /// - Parameters:
    ///   - url: Server URL of the form ws[s]://[username:password@]host[:port]/channel
    ///   - clientID: A unique client identifier.
    ///   - options: Eftl options.
    public convenience init(url: String, clientID: String?, options: EftlOptions = EftlOptions()) {
        self.init(url: url, clientID: clientID, options: options, withDelegate: nil)
    }
    
    deinit {
        self.delegate = nil
        self.disconnect()
    }
    
    // MARK: Methods
    
    // Start a connection to the server.
    open func connect() {
        dispatchQueue.async {
            guard self.state == .disconnected else { return }
            self.state = .connecting
            self.reconnectAttempts = 0
            self.urlIndex = 0
            self.reconnectTask?.cancel()
            self.webSocketConnect(self.currentURL())
        }
    }
    
    /// Disconnect from the server.
    open func disconnect() {
        dispatchQueue.async {
            guard self.state != .disconnected else { return }
            self.state = .disconnected
            self.reconnectTask?.cancel()
            self.webSocketWrite(["op": OpCode.disconnect.rawValue])
            self.webSocketDisconnect()
        }
    }
    
    /// Publish a message.
    ///
    /// - Parameters:
    ///   - message: Message to publish.
    ///   - completion: Called when the publish completes.
    open func publish(_ message: EftlMessage, onCompletion completion: ((Error?) -> Void)? = nil) {
        dispatchQueue.async {
            guard self.state != .disconnected else {
                DispatchQueue.main.async { completion?(EftlError.notConnected(reason: "not connected")) }
                return
            }
            let seq = self.nextSeq()
            let json: JsonType = ["op": OpCode.publish.rawValue,
                                  "seq": seq,
                                  "body": message.encode()]
            self.requests[seq] = Request(json: json, completion: completion)
            self.webSocketWrite(json)
        }
    }
    
    /// Subscribe to messages.
    ///
    /// Subscription matchers are content-based, meaning that only messages
    /// with matching content will be received. A subscription matcher
    /// can match string and integer message fields, and check for the existence
    /// or absence of a message field.
    ///
    /// For example, the following matcher
    ///
    ///     `["symbol": "aapl", "order": 1000, "expired": false]`
    ///
    /// will match all messages containing a message field "symbol" of string value
    /// "aapl", a mesasge field "order" of integer value 1000, and does NOT contain
    /// a message field "expired".
    ///
    /// - Parameters:
    ///   - matcher: Message content matcher, or `[:]` to receive all messages.
    ///   - durable: Durable subscription name.
    ///   - callback: Called when a matched message is received.
    ///   - completion: Called when the subscription completes.
    /// - Returns: The subscription identifier.
    @discardableResult
    open func subscribe(_ matcher: JsonType, durable: String?, onMessage callback: ((EftlMessage) -> Void)?, onCompletion completion: ((Error?) -> Void)? = nil) -> EftlSubscription? {
        return subscribe(matcher, durable: durable, options: EftlSubscriptionOptions(), onMessage: callback, onCompletion: completion)
    }
    
    /// Subscribe to messages.
    ///
    /// Subscription matchers are content-based, meaning that only messages
    /// with matching content will be received. A subscription matcher
    /// can match string and integer message fields, and check for the existence
    /// or absence of a message field.
    ///
    /// For example, the following matcher
    ///
    ///     `["symbol": "aapl", "order": 1000, "expired": false]`
    ///
    /// will match all messages containing a message field "symbol" of string value
    /// "aapl", a mesasge field "order" of integer value 1000, and does NOT contain
    /// a message field "expired".
    ///
    /// - Parameters:
    ///   - matcher: Message content matcher, or `[:]` to receive all messages.
    ///   - durable: Durable subscription name.
    ///   - options: Subscription options.
    ///   - callback: Called when a matched message is received.
    ///   - completion: Called when the subscription completes.
    /// - Returns: The subscription identifier.
    @discardableResult
    open func subscribe(_ matcher: JsonType, durable: String?, options: EftlSubscriptionOptions, onMessage callback: ((EftlMessage) -> Void)?, onCompletion completion: ((Error?) -> Void)? = nil) -> EftlSubscription? {
        return dispatchQueue.sync {
            guard self.state != .disconnected else {
                DispatchQueue.main.async { completion?(EftlError.notConnected(reason: "not connected")) }
                return nil
            }
            let sid = nextSid()
            var json: JsonType = ["op": OpCode.subscribe.rawValue,
                                  "id": sid,
                                  "matcher": JsonStringify(matcher)]
            json["durable"] = durable
            if options.type != .standard {
                json["type"] = options.type.rawValue
                json["key"] = options.key
            }
            self.subscriptions[sid] = Request(json: json, callback: { message in
                if let message = message {
                    callback?(message)
                }
            }, completion: completion)
            self.webSocketWrite(json)
            return EftlSubscription(sid)
        }
    }
    
    /// Remove a subscription.
    ///
    /// - Parameter subscription: The subscription identifier to remove.
    open func unsubscribe(_ subscription: EftlSubscription) {
        dispatchQueue.async {
            self.subscriptions.removeValue(forKey: subscription)
            self.webSocketWrite(["op": OpCode.unsubscribe.rawValue,
                                 "id": subscription])
        }
    }
    
    /// Remove all subscriptions.
    open func unsubscribeAll() {
        dispatchQueue.async {
            for (subscription, _) in self.subscriptions {
                self.webSocketWrite(["op": OpCode.unsubscribe.rawValue,
                                     "id": subscription])
            }
            self.subscriptions.removeAll()
        }
    }
    
    /// Return a named key-value map.
    ///
    /// - Parameter name: The map name.
    /// - Returns: The named key-value map.
    open func map(_ name: String) -> EftlMap {
        return EftlMap(name, eftl: self)
    }
    
    // MARK: Private
    
    private let options: EftlOptions
    private let urlList: [String]
    private var urlIndex: Int = 0
    private var clientID: String?
    private var token: String?
    private var maxSize: Int? // TODO: enforce max message size
    private var reconnectTask: DispatchWorkItem?
    private var reconnectAttempts: Int = 0
    private var sidCounter: UInt64 = 0
    private var seqCounter: UInt64 = 0
    private var lastSeqNum: UInt64 = 0
    
    private var subscriptions = [String: Request]()
    fileprivate var requests = [UInt64: Request]()
    
    private var socket: WebSocket?
    
    fileprivate let dispatchQueue = DispatchQueue(label: "com.tibco.eftl.DispatchQueue", attributes: [])
    
    fileprivate var state: State = .disconnected
    
    fileprivate enum State {
        case disconnected
        case connecting
        case connected
    }
    
    fileprivate enum OpCode: Int {
        case heartbeat = 0
        case login = 1
        case welcome = 2
        case subscribe = 3
        case subscribed = 4
        case unsubscribe = 5
        case unsubscribed = 6
        case message = 7
        case publish = 8
        case ack = 9
        case error = 10
        case disconnect = 11
        case mapSet = 20
        case mapGet = 22
        case mapRemove = 24
        case mapResponse = 26
    }
    
    fileprivate class Request {
        let json: JsonType
        
        private let callback: ((EftlMessage?) -> Void)?
        private var completion: ((Error?) -> Void)?
        
        init(json: JsonType, callback: ((EftlMessage?) -> Void)? = nil, completion: ((Error?) -> Void)?) {
            self.json = json
            self.callback = callback
            self.completion = completion
        }
        
        func onCompletion(_ error: Error?) {
            completion?(error)
            completion = nil
        }
        
        func onMessage(_ message: EftlMessage?) {
            callback?(message)
        }
    }
    
    fileprivate func nextSeq() -> UInt64 {
        seqCounter += 1
        return seqCounter
    }
    
    private func nextSid() -> String {
        sidCounter += 1
        return String(sidCounter)
    }
    
    private func onConnect() {
        let opts: JsonType = ["_qos": "true", "_resume": "true"]
        var json: JsonType = ["op": OpCode.login.rawValue,
                              "client_type": "swift",
                              "client_version": Eftl.version,
                              "login_options": opts]
        json["client_id"] = clientID
        json["id_token"] = token
        json["user"] = options.username
        json["password"] = options.password
        // device token for APNs notifications
        if let deviceToken = options.deviceToken, deviceToken.count > 0 {
            json["notification_token"] = options.deviceToken
            json["notification_type"] = "apns"
        }
        webSocketWrite(json)
    }
    
    private func onDisconnect(_ error: Error?) {
        var err = error
        if let e = err as? WSError {
            switch e.code {
            case Int(CloseCode.goingAway.rawValue):
                err = EftlError.goingAway(reason: e.message)
            case Int(CloseCode.messageTooBig.rawValue):
                err = EftlError.messageTooBig(reason: e.message)
            default:
                err = EftlError.connectionError(code: e.code, reason: e.message)
            }
        }
        
        let expected = state == .disconnected
        
        // connect if there are still URLs in the list
        //
        // autoreconnect if not intentially disconnecting and remaining attempts
        if state == .connecting, let url = nextURL() {
            webSocketConnect(url)
        } else if state == .connected && reconnectAttempts < options.reconnectMaxAttempts {
            if let url = nextURL() {
                webSocketConnect(url)
            } else {
                let url = currentURL()
                // exponential backoff with a randomness factor of 0.5
                let jitter = Double.random(in: 0.5..<1.5)
                let timeInterval = min(pow(Double(2), Double(reconnectAttempts))*jitter, options.reconnectMaxInterval)
                
                let reconnectTask = DispatchWorkItem { [weak self] in
                    self?.webSocketConnect(url)
                }
                dispatchQueue.asyncAfter(wallDeadline: .now() + timeInterval, execute: reconnectTask)
                reconnectAttempts += 1
                // notify delegate of reconnect
                DispatchQueue.main.async {
                    self.delegate?.eftlWillReconnect(self, withError: err, afterDelay: timeInterval)
                }
            }
        } else {
            state = .disconnected
            // notify delegate of disconnect
            DispatchQueue.main.async {
                self.delegate?.eftlDidDisconnect(self, withError: expected ? nil : err)
            }
            // cancel pending requests
            for (_, req) in requests {
                DispatchQueue.main.async {
                    req.onCompletion(err)
                }
            }
            requests.removeAll()
        }
    }
    
    private func onMessage(_ json: JsonType) {
        guard let op = json["op"] as? Int else { return }
        switch OpCode(rawValue: op) {
        case .heartbeat?:
            handleHeartbeat(json)
        case .welcome?:
            handleWelcome(json)
        case .message?:
            handleMessage(json)
        case .subscribed?:
            handleSubscribed(json)
        case .unsubscribed?:
            handleUnsubscribed(json)
        case .ack?:
            handleAck(json)
        case .mapResponse?:
            handleMapResponse(json)
        default:
            break
        }
    }
    
    private func handleHeartbeat(_ json: JsonType) {
        webSocketWrite(json)
    }
    
    private func handleWelcome(_ json: JsonType) {
        clientID = json["client_id"] as? String
        token = json["id_token"] as? String
        maxSize = json["max_size"] as? Int
        // reset auto-reconnect attempts
        state = .connected
        reconnectAttempts = 0
        urlIndex = 0
        // check for resumed session
        if let resume = json["_resume"] as? String {
            if resume.lowercased() != "true" {
                lastSeqNum = 0
            }
        }
        // repair subscriptions
        for (_, sub) in subscriptions {
            webSocketWrite(sub.json)
        }
        // re-send unacknowledged messages
        for (_, req) in requests {
            webSocketWrite(req.json)
        }
        // notify delegate of connect
        DispatchQueue.main.async {
            self.delegate?.eftlDidConnect(self)
        }
    }
    
    private func handleMessage(_ json: JsonType) {
        guard let seq = json["seq", default: UInt64(0)] as? UInt64 else { return }
        defer {
            if seq > 0 {
                lastSeqNum = seq
                // message is acknowledged regardless of outcome
                webSocketWrite(["op": OpCode.ack.rawValue,
                                "seq": seq])
            }
        }
        guard let sid = json["to"] as? String else { return }
        guard let msg = json["body"] as? JsonType else { return }
        if seq == 0 || seq > lastSeqNum {
            if let sub = subscriptions[sid] {
                DispatchQueue.main.async {
                    sub.onMessage(EftlMessage.decode(msg))
                }
            }
        }
    }
    
    private func handleSubscribed(_ json: JsonType) {
        guard let sid = json["id"] as? String else { return }
        guard let sub = subscriptions[sid] else { return }
        DispatchQueue.main.async {
            sub.onCompletion(nil)
        }
    }
    
    private func handleUnsubscribed(_ json: JsonType) {
        guard let sid = json["id"] as? String else { return }
        guard let sub = subscriptions.removeValue(forKey: sid) else { return }
        let code = json["err"] as? Int ?? 0
        let reason = json["reason"] as? String ?? "subscribe failed"
        var error: Error
        if code == 13 {
            error = EftlError.notAuthorized(reason: reason)
        } else if code == 22 {
            error = EftlError.invalidSubscription(reason: reason)
        } else {
            error = EftlError.subscribeError(reason: reason)
        }
        DispatchQueue.main.async {
            sub.onCompletion(error)
        }
    }
    
    private func handleAck(_ json: JsonType) {
        guard let seq = json["seq"] as? UInt64 else { return }
        guard let req = requests.removeValue(forKey: seq) else { return }
        var error: Error? = nil
        if let code = json["err"] as? Int {
            let reason = json["reason"] as? String ?? "publish failed"
            if code == 12 {
                error = EftlError.notAuthorized(reason: reason)
            } else {
                error = EftlError.publishError(reason: reason)
            }
        }
        DispatchQueue.main.async {
            req.onCompletion(error)
        }
    }
    
    private func handleMapResponse(_ json: JsonType) {
        guard let seq = json["seq"] as? UInt64 else { return }
        guard let req = requests.removeValue(forKey: seq) else { return }
        if let msg = json["value"] as? JsonType {
            DispatchQueue.main.async {
                req.onMessage(EftlMessage.decode(msg))
            }
        } else {
            DispatchQueue.main.async {
                req.onMessage(nil)
            }
        }
        var error: Error? = nil
        if let code = json["err"] as? Int {
            let reason = json["reason"] as? String ?? "map request failed"
            if code == 14 {
                error = EftlError.notAuthorized(reason: reason)
            } else {
                error = EftlError.mapError(reason: reason)
            }
        }
        DispatchQueue.main.async {
            req.onCompletion(error)
        }
    }
}

fileprivate extension Eftl {
    func nextURL() -> URL? {
        urlIndex += 1
        if urlIndex < urlList.count {
            return URL(string: urlList[urlIndex])
        }
        urlIndex = 0
        return nil
    }
    
    func currentURL() -> URL? {
        return URL(string: urlList[urlIndex])
    }
    
    func resetURL() {
        urlIndex = 0
    }
    
    func webSocketConnect(_ url: URL?) {
        guard let url = url else {
            DispatchQueue.main.async {
                self.delegate?.eftlDidDisconnect(self, withError: EftlError.invalidURL(reason: self.urlList.joined(separator: "|")))
            }
            return
        }
        socket = WebSocket(url: url, protocols:  ["v1.eftl.tibco.com"])
        socket?.security = SSLSecurity(data: options.sslTrustedCertificates)
        socket?.disableSSLCertValidation = options.sslAllowUntrustedCertificates
        socket?.callbackQueue = dispatchQueue
        socket?.onConnect = onWebSocketConnect
        socket?.onDisconnect = onWebSocketDisconnect
        socket?.onText = onWebSocketText
        socket?.connect()
    }
    
    func webSocketDisconnect() {
        socket?.disconnect()
    }
    
    func webSocketWrite(_ json: JsonType) {
        guard let data = try? JSONSerialization.data(withJSONObject: json, options: []) else { return }
        guard let text = String(data: data, encoding: .utf8) else { return }
        socket?.write(string: text)
    }
    
    func onWebSocketConnect() {
        onConnect()
    }
    
    func onWebSocketDisconnect(error: Error?) {
        onDisconnect(error)
    }
    
    func onWebSocketText(text: String) {
        if let json = try? JSONSerialization.jsonObject(with: Data(text.utf8), options: []) as? JsonType {
            onMessage(json)
        }
    }
}

/// An EftlMap object representing a named key-value map.
public struct EftlMap {
    
    // MARK: Properties
    
    /// The key-value map name.
    public let name: String
    
    // MARK: Initializers
    
    init(_ name: String, eftl: Eftl) {
        self.name = name
        self.eftl = eftl
    }
    
    // MARK: Methods
    
    /// Set a key's value in the named key-value map.
    ///
    /// - Parameters:
    ///   - key: The key to set.
    ///   - value: The value to store in the key.
    ///   - completion: Called when the map operation completes.
    public func setKey(_ key: String, toValue value: EftlMessage, onCompletion completion: ((Error?) -> Void)? = nil) {
        guard let eftl = eftl else { return }
        eftl.dispatchQueue.async {
            guard eftl.state != .disconnected else {
                DispatchQueue.main.async { completion?(EftlError.notConnected(reason: "not connected")) }
                return
            }
            let seq = eftl.nextSeq()
            let json: JsonType = ["op": Eftl.OpCode.mapSet.rawValue,
                                  "seq": seq,
                                  "map": self.name,
                                  "key": key,
                                  "value": value.fields]
            eftl.requests[seq] = Eftl.Request(json: json, completion: completion)
            eftl.webSocketWrite(json)
        }
    }
    
    /// Get a key's value from the named key-value map.
    ///
    /// - Parameters:
    ///   - key: The key to get.
    ///   - callback: Called with the key's stored value, or `nil` if the key is not set.
    ///   - completionHandler: Called when the map operation completes.
    public func getKey(_ key: String, onValue callback: ((EftlMessage?) -> Void)?, onCompletion completion: ((Error?) -> Void)? = nil) {
        guard let eftl = eftl else { return }
        eftl.dispatchQueue.async {
            guard eftl.state != .disconnected else {
                DispatchQueue.main.async { completion?(EftlError.notConnected(reason: "not connected")) }
                return
            }
            let seq = eftl.nextSeq()
            let json: JsonType = ["op": Eftl.OpCode.mapGet.rawValue,
                                  "seq": seq,
                                  "map": self.name,
                                  "key": key]
            eftl.requests[seq] = Eftl.Request(json: json, callback: callback, completion: completion)
            eftl.webSocketWrite(json)
        }
    }
    
    /// Remove a key from the named key-value map.
    ///
    /// - Parameters:
    ///   - key: The key to remove.
    ///   - completionHandler: Called when the map operation completes.
    public func removeKey(_ key: String, onCompletion completion: ((Error?) -> Void)? = nil) {
        guard let eftl = eftl else { return }
        eftl.dispatchQueue.async {
            guard eftl.state != .disconnected else {
                DispatchQueue.main.async { completion?(EftlError.notConnected(reason: "not connected")) }
                return
            }
            let seq = eftl.nextSeq()
            let json: JsonType = ["op": Eftl.OpCode.mapRemove.rawValue,
                                  "seq": seq,
                                  "map": self.name,
                                  "key": key]
            eftl.requests[seq] = Eftl.Request(json: json, completion: completion)
            eftl.webSocketWrite(json)
        }
    }
    
    // MARK: Private
    
    private weak var eftl: Eftl?
}

/// An EftlMessage object.
public struct EftlMessage: Sequence {
    
    // MARK: Properties
    
    public var fields = JsonType()
    
    // MARK: Initializers
    
    /// Create a new message.
    public init(_ json: JsonType = [:]) {
        for (k, v) in json {
            fields[k] = v
        }
    }
    
    // MARK: Methods
    
    public subscript(index: String) -> Any? {
        get { return getField(index) }
        set { setField(index, toValue: newValue) }
    }
    
    public func makeIterator() -> Dictionary<String, Any>.Iterator {
        return fields.makeIterator()
    }
    
    /// Set the value of the named message field. A value of `nil` will remove the field.
    ///
    /// - Parameters:
    ///   - name: The named message field to set.
    ///   - value: The value to set in the named message field.
    public mutating func setField(_ field: String, toValue value: Any?) {
        fields[field] = value
    }
    
    /// Get the value of the named message field.
    ///
    /// - Parameter field: The named message field to get.
    /// - Returns: The message field value, or `nil` if not set.
    public func getField(_ field: String) -> Any? {
        return fields[field]
    }
    
    func encode() -> JsonType {
        var enc = JsonType()
        for (key, val) in fields {
            switch val {
            case let val as [UInt8]:
                var data = Data()
                data.append(contentsOf: val)
                enc[key] = ["_o_": data.base64EncodedString()]
            case is Float, is Double:
                enc[key] = ["_d_": val]
            case let val as [Float]:
                var arr = [[String:Float]]()
                for itm in val {
                    arr.append(["_d_": itm])
                }
                enc[key] = arr
            case let val as [Double]:
                var arr = [JsonType]()
                for itm in val {
                    arr.append(["_d_": itm])
                }
                enc[key] = arr
            case let val as Date:
                enc[key] = ["_m_": val.timeIntervalSince1970 * 1000.0]
            case let val as [Date]:
                var arr = [JsonType]()
                for itm in val {
                    arr.append(["_m_": itm.timeIntervalSince1970 * 1000.0])
                }
                enc[key] = arr
            case let val as JsonType:
                enc[key] = EftlMessage(val).encode()
            case let val as [JsonType]:
                var arr = [JsonType]()
                for itm in val {
                    arr.append(EftlMessage(itm).encode())
                }
                enc[key] = arr
            case let val as EftlMessage:
                enc[key] = val.encode()
            case let val as [EftlMessage]:
                var arr = [JsonType]()
                for itm in val {
                    arr.append(itm.encode())
                }
                enc[key] = arr
            default:
                enc[key] = val
            }
        }
        return enc
    }
    
    static func decode(_ json: JsonType) -> EftlMessage{
        var dec = JsonType()
        for (key, val) in json {
            switch val {
            case let val as JsonType:
                if let opaque = val["_o_"] as? String {
                    var bytes = [UInt8]()
                    if let data = Data(base64Encoded: opaque) {
                        bytes.append(contentsOf: data)
                    }
                    dec[key] = bytes
                } else if let double = val["_d_"] as? Double {
                    dec[key] = double
                } else if let millis = val["_m_"] as? Double {
                    dec[key] = Date(timeIntervalSince1970: millis/1000.0)
                } else {
                    dec[key] = EftlMessage.decode(val)
                }
            case let val as [JsonType]:
                var arr = [Any]()
                for itm in val {
                    if let double = itm["_d_"] as? Double {
                        arr.append(double)
                    } else if let millis = itm["_m_"] as? Double {
                        arr.append(Date(timeIntervalSince1970: millis/1000.0))
                    } else {
                        arr.append(EftlMessage.decode(itm))
                    }
                }
                dec[key] = arr
            default:
                dec[key] = val
            }
        }
        return EftlMessage(dec)
    }
}

extension EftlMessage: CustomStringConvertible {
    public var description: String {
        var desc: String = "{"
        for (key, val) in fields {
            if desc.count > 1 {
                desc.append(", ")
            }
            desc.append(key)
            desc.append(":")
            desc.append("\(val)")
        }
        desc.append("}")
        return desc
    }
}

/// Eftl subscription identifier.
public typealias EftlSubscription = String

/// Eftl subscription options.
public struct EftlSubscriptionOptions {
    /// Durable subscription types.
    public enum SubscriptionType: String {
        case standard = "standard"
        case shared = "shared"
        case lastValue = "last-value"
    }
    
    /// Durable subscription type.
    public let type: SubscriptionType
    /// Key field required by last-value subscriptions.
    public let key: String?
    
    public init(type: SubscriptionType = .standard, key: String? = nil) {
        self.type = type
        self.key = key
    }
}

/// Eftl errors.
public enum EftlError: Error {
    /// The URL is invalid.
    case invalidURL(reason: String)
    /// The operation could not complete as there is no connection to the server.
    case notConnected(reason: String)
    /// The server is shutting down.
    case goingAway(reason: String)
    /// The published message exceeds the configured maximum message size.
    case messageTooBig(reason: String)
    /// The user credentials could not be authenticated.
    case notAuthenticated(reason: String)
    /// The user credentials are not authorized for the operation.
    case notAuthorized(reason: String)
    /// The connection was forcibly closed by the server.
    case closed(reason: String)
    /// The subscription's durable or matcher is invalid.
    case invalidSubscription(reason: String)
    /// The subscribe operation failed.
    case subscribeError(reason: String)
    /// The publish operation failed.
    case publishError(reason: String)
    /// The map operation failed.
    case mapError(reason: String)
    /// The connection failed.
    case connectionError(code: Int, reason: String)
}

extension EftlError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .invalidURL(let reason),
             .notConnected(let reason),
             .goingAway(let reason),
             .messageTooBig(let reason),
             .notAuthenticated(let reason),
             .notAuthorized(let reason),
             .closed(let reason),
             .invalidSubscription(let reason),
             .subscribeError(let reason),
             .publishError(let reason),
             .mapError(let reason):
            return reason
        case .connectionError(let code, let reason):
            return "\(code): \(reason)"
        }
    }
}

func JsonStringify(_ value: Any, defaultValue: String = "") -> String {
    if JSONSerialization.isValidJSONObject(value) {
        do {
            let data = try JSONSerialization.data(withJSONObject: value, options: [])
            let string = String(data: data, encoding: .utf8)
            if string != nil {
                return string!
            }
        } catch _ {
        }
    }
    return defaultValue
}

extension SSLSecurity {
    convenience init?(data: [Data]?) {
        guard let data = data else { return nil }
        var certs = [SSLCert]()
        for item in data {
            certs.append(SSLCert(data: item))
        }
        self.init(certs: certs, usePublicKeys: false)
    }
}

extension URL {
    var queryParameters: [String: String]? {
        guard
            let components = URLComponents(url: self, resolvingAgainstBaseURL: true),
            let queryItems = components.queryItems else { return nil }
        return queryItems.reduce(into: [String: String]()) { (result, item) in
            result[item.name] = item.value
        }
    }
}

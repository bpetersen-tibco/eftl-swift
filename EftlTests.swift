//
//  EftlTests.swift
//  EftlTests
//
//  Created by Brent Petersen on 4/30/19.
//  Copyright © 2019 TIBCO Software. All rights reserved.
//

import XCTest
@testable import Eftl

let url: String = "wss://127.0.0.1:9090|wss://127.0.0.1:9191/channel|wss://127.0.0.1:9292/channel"
let username: String = "user"
let password: String = "pass"
let clientID: String = "test"

let timeout: TimeInterval = 5
let matcher: JsonType = [
    "text": "Hello, World!",
    "long": 42,
    "double": true,
    "now": true,
    "binary": true,
    "long-array": true,
    "double-array": true,
    "now-array": true,
    "message-array": true]
let message: JsonType = [
    "text": "Hello, World!",
    "long": 42,
    "double": 0.1,
    "now": Date(),
    "binary": [UInt8](arrayLiteral: 97, 98, 99, 100),
    "long-array": [1, 2, 3],
    "double-array": [0.1, 0.2, 0.3],
    "now-array": [Date(), Date(), Date()],
    "message-array": [EftlMessage(["name":"joe"]), EftlMessage(["name":"bob"])]]
let certificate = "MIICAjCCAYmgAwIBAgIBATAKBggqhkjOPQQDBDA6MTgwNgYDVQQDDC9UUE9SVC1ST09UOjEzOWQ4YjM0LTEyNTctNGJmMi04NWNjLWM3MWU1MjJmYmY1OTAeFw0xOTA2MTEwNzIyMDJaFw0zODAxMTkwMzE0MDdaMDoxODA2BgNVBAMML1RQT1JULVJPT1Q6MTM5ZDhiMzQtMTI1Ny00YmYyLTg1Y2MtYzcxZTUyMmZiZjU5MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAEWx/Z2GvaphvY2aU3gDfZurEbyYPz2TrtutpPgp2C0qTPoXaYgAUmJInwOxmWAkBEiQlxEkxRmU3sPI7v/HxpCpU8YYnnZg9iTu2vIVLNf8R2smpCjjgZiYaTR9R6UHC2o2MwYTAPBgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB/wQEAwICBDAdBgNVHQ4EFgQU90RvZFt30d7jaPf9QsB/LAEY4G0wHwYDVR0jBBgwFoAU90RvZFt30d7jaPf9QsB/LAEY4G0wCgYIKoZIzj0EAwQDZwAwZAIwB7esUIQLl+eWp+SaK7c5yv1b/0U5W965c0fA+eQEgUOw3iPk/wmkczsFiLxYzNIGAjAfr5BVrNH+/XqkS0DAFKby5llMyPw84KAxhnpwUcmZwFotc2L6zVpkEQnwNeBEJsE="

class EftlTests: XCTestCase, EftlDelegate {
    
    var text: String = ""
    
    var eftl: Eftl?
    
    var connectExp: XCTestExpectation?
    var disconnectExp: XCTestExpectation?
    
    override func setUp() {
        // Put setup code here. This method is called before the invocation of each test method in the class.
        let data = Data(base64Encoded: certificate)
        let opts = EftlOptions(
            username: username,
            password: password,
            //sslTrustedCertificates: [data!],
            sslAllowUntrustedCertificates: true)
        eftl = Eftl(url: url, clientID: clientID, options: opts, withDelegate: self)
    }
    
    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }
    
    func testConnect() {
        connectExp = XCTestExpectation(description: "connect")
        disconnectExp = XCTestExpectation(description: "disconnect")
        
        // connect
        eftl?.connect()
        
        wait(for: [connectExp!], timeout: timeout)
        
        XCTAssert(eftl!.isConnected)
        
        // disconnect
        eftl?.disconnect()
        wait(for: [disconnectExp!], timeout: timeout)

        connectExp = XCTestExpectation(description: "connect")
        disconnectExp = XCTestExpectation(description: "disconnect")
        
        // reconnect
        eftl?.connect()
        
        wait(for: [connectExp!], timeout: timeout)
        
        XCTAssert(eftl!.isConnected)
        
        // final disconnect
        eftl?.disconnect()
        wait(for: [disconnectExp!], timeout: timeout)
    }
    
    func testPubSub() {
        
        connectExp = XCTestExpectation(description: "connect")
        disconnectExp = XCTestExpectation(description: "disconnect")
        
        let messageExp = XCTestExpectation(description: "message")
        
        // connect
        eftl?.connect()
        
        wait(for: [connectExp!], timeout: timeout)
        
        XCTAssert(eftl!.isConnected)
        
        // subscribe
        let sub = eftl?.subscribe(matcher, durable: "foo", onMessage: { message in
            XCTAssert(message["text"] as? String == "Hello, World!", "message string field invalid")
            XCTAssert(message["long"] as? Int == 42, "message long field invalid")
            messageExp.fulfill()
        }) { error in
            if let error = error {
                XCTFail(error.localizedDescription)
            }
        }
        
        // publish
        eftl?.publish(EftlMessage(message)) { error in
            if let error = error {
                XCTFail(error.localizedDescription)
            }
        }
        
        // wait for message to be received
        wait(for: [messageExp], timeout: timeout)
        
        // unsubscribe
        if let sub = sub {
            eftl?.unsubscribe(sub)
        }
        
        // disconnect
        eftl?.disconnect()
        wait(for: [disconnectExp!], timeout: timeout)
    }
    
    func testMap() {
        connectExp = XCTestExpectation(description: "connect")
        disconnectExp = XCTestExpectation(description: "disconnect")
        
        let setExp = XCTestExpectation(description: "set map")
        setExp.expectedFulfillmentCount = 3
        
        // connect
        eftl?.connect()
        
        wait(for: [connectExp!], timeout: timeout)
        
        XCTAssert(eftl!.isConnected)
        
        let map = eftl?.map("foo")
        
        map?.setKey("key1", toValue: EftlMessage(["key": 1])) { error in
            if let error = error {
                XCTFail(error.localizedDescription)
            }
            setExp.fulfill()
        }
        map?.setKey("key2", toValue: EftlMessage(["key": 2])) { error in
            if let error = error {
                XCTFail(error.localizedDescription)
            }
            setExp.fulfill()
        }
        map?.setKey("key3", toValue: EftlMessage(["key": 3])) { error in
            if let error = error {
                XCTFail(error.localizedDescription)
            }
            setExp.fulfill()
        }
        
        wait(for: [setExp], timeout: timeout)
        
        let getExp = XCTestExpectation(description: "get map")
        getExp.expectedFulfillmentCount = 3
        
        map?.getKey("key1", onValue: { message in
            if let message = message {
                XCTAssert(message.getField("key") as? Int == 1, "invalid message value")
            } else {
                XCTFail("message is nil")
            }
        }) { error in
            if let error = error {
                XCTFail(error.localizedDescription)
            }
            getExp.fulfill()
        }
        map?.getKey("key2", onValue: { message in
            if let message = message {
                XCTAssert(message.getField("key") as? Int == 2, "invalid message value")
            } else {
                XCTFail("message is nil")
            }
        }) { error in
            if let error = error {
                XCTFail(error.localizedDescription)
            }
            getExp.fulfill()
        }
        map?.getKey("key3", onValue: { message in
            if let message = message {
                XCTAssert(message.getField("key") as? Int == 3, "invalid message value")
            } else {
                XCTFail("message is nil")
            }
        }) { error in
            if let error = error {
                XCTFail(error.localizedDescription)
            }
            getExp.fulfill()
        }
        
        wait(for: [getExp], timeout: timeout)
        
        let removeExp = XCTestExpectation(description: "remove map")
        removeExp.expectedFulfillmentCount = 3
        
        map?.removeKey("key1") { error in
            if let error = error {
                XCTFail(error.localizedDescription)
            }
            removeExp.fulfill()
        }
        map?.removeKey("key2") { error in
            if let error = error {
                XCTFail(error.localizedDescription)
            }
            removeExp.fulfill()
        }
        map?.removeKey("key3") { error in
            if let error = error {
                XCTFail(error.localizedDescription)
            }
            removeExp.fulfill()
        }
        
        wait(for: [removeExp], timeout: timeout)
        
        // disconnect
        eftl?.disconnect()
        wait(for: [disconnectExp!], timeout: 5.0)
    }
    
    func eftlDidConnect(_ eftl: Eftl) {
        connectExp?.fulfill()
    }
    
    func eftlDidDisconnect(_ eftl: Eftl, withError error: Error?) {
        if let error = error {
            XCTFail(error.localizedDescription)
        }
        disconnectExp?.fulfill()
    }
    
    func eftlWillReconnect(_ eftl: Eftl, withError error: Error?, afterDelay delay: TimeInterval) {
        if let error = error {
            XCTFail(error.localizedDescription)
        } else {
            print("will reconnect")
        }
    }
}

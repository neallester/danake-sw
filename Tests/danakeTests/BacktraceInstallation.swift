//
//  BacktraceInstallation.swift
//  Backtrace
//
//  Created by Neal Lester on 8/4/19.
//

import Foundation
import Backtrace

class BacktraceInstallation {
    
    static func install() {
        if !backtraceInstalled {
            backtraceInstalled = true
            Backtrace.install()
        }
    }
    
    static var backtraceInstalled = false
    
    
}

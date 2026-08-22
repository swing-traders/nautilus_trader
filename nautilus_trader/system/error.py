# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------


class KernelStartupError(Exception):
    """
    The base class for all kernel startup errors.

    A startup stage which does not complete leaves the system connected but unable to
    trade, so each stage raises rather than returning to a caller which cannot tell the
    difference.

    """


class EngineConnectionTimeout(KernelStartupError):
    """
    Raised when the engines do not connect and initialize within the timeout.
    """


class ExecutionReconciliationFailed(KernelStartupError):
    """
    Raised when execution state could not be reconciled.
    """


class PortfolioInitializationTimeout(KernelStartupError):
    """
    Raised when the portfolio does not initialize within the timeout.
    """

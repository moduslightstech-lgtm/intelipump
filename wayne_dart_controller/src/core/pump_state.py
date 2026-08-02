import time
import queue
import logging

class SaleLifecycle:
    IDLE = "IDLE"
    NOZZLE_LIFTED = "NOZZLE_LIFTED"
    AUTHORIZED = "AUTHORIZED"
    FILLING = "FILLING"
    COMPLETED = "COMPLETED"
    NOZZLE_RETURNED = "NOZZLE_RETURNED"
    ABORTED = "ABORTED"
    CLOSED = "CLOSED"

LEGAL_LIFECYCLE_TRANSITIONS = {
    SaleLifecycle.IDLE: [SaleLifecycle.NOZZLE_LIFTED, SaleLifecycle.ABORTED],
    SaleLifecycle.NOZZLE_LIFTED: [SaleLifecycle.AUTHORIZED, SaleLifecycle.NOZZLE_RETURNED, SaleLifecycle.ABORTED, SaleLifecycle.IDLE],
    SaleLifecycle.AUTHORIZED: [SaleLifecycle.FILLING, SaleLifecycle.NOZZLE_RETURNED, SaleLifecycle.ABORTED, SaleLifecycle.IDLE],
    SaleLifecycle.FILLING: [SaleLifecycle.COMPLETED, SaleLifecycle.NOZZLE_RETURNED, SaleLifecycle.ABORTED],
    SaleLifecycle.COMPLETED: [SaleLifecycle.NOZZLE_RETURNED, SaleLifecycle.CLOSED, SaleLifecycle.IDLE],
    SaleLifecycle.NOZZLE_RETURNED: [SaleLifecycle.IDLE, SaleLifecycle.CLOSED],
    SaleLifecycle.ABORTED: [SaleLifecycle.IDLE],
    SaleLifecycle.CLOSED: [SaleLifecycle.IDLE]
}

class PumpState:
    """Isolated, edge-deduplicated state container for a single pump address."""

    def __init__(self, address: int):
        self.address = address
        self.synchronized = False
        self.online = False
        self.consecutive_missed_polls = 0
        
        self.observed_status = "UNKNOWN"
        self.nozzle_position = "UNKNOWN"
        self.logical_nozzle = 1
        self.unit_price = 0.0
        self.filled_volume = 0.0
        self.filled_amount = 0.0
        
        self.seq_num = 0x30
        self.sale_lifecycle = SaleLifecycle.IDLE
        self.sale_completion_latch = False
        self.authorization_pending = False
        
        self.event_queue = queue.Queue()

        # Strict Monotonic Timestamps for State Correlation
        self.last_status_time = 0.0
        self.last_nozzle_time = 0.0
        self.last_valid_frame_time = 0.0
        self.last_command_time = 0.0
        self.last_ack_time = 0.0

    def get_reserved_seq_ctrl(self) -> int:
        return self.seq_num

    def advance_seq_ctrl(self):
        self.seq_num = 0x30 + ((self.seq_num - 0x30 + 1) % 16)

    def transition_lifecycle(self, target_state: str) -> bool:
        if target_state == self.sale_lifecycle:
            return True
            
        allowed = LEGAL_LIFECYCLE_TRANSITIONS.get(self.sale_lifecycle, [])
        if target_state in allowed:
            logging.info(f"[LIFECYCLE {hex(self.address)}] {self.sale_lifecycle} -> {target_state}")
            self.sale_lifecycle = target_state
            return True
        else:
            logging.error(f"[REJECTED TRANSITION {hex(self.address)}] Illegal transition attempted: {self.sale_lifecycle} -> {target_state}")
            return False

    def update_nozzle(self, new_pos: str, nozzle_num: int, obs_time: float) -> bool:
        self.last_nozzle_time = obs_time
        self.last_valid_frame_time = obs_time

        if self.nozzle_position == "UNKNOWN":
            self.nozzle_position = new_pos
            self.logical_nozzle = nozzle_num
            return False

        if self.nozzle_position != new_pos or self.logical_nozzle != nozzle_num:
            self.nozzle_position = new_pos
            self.logical_nozzle = nozzle_num
            
            if new_pos == "OUT":
                self.transition_lifecycle(SaleLifecycle.NOZZLE_LIFTED)
                self.sale_completion_latch = False
            elif new_pos == "IN":
                if self.sale_lifecycle == SaleLifecycle.COMPLETED:
                    self.transition_lifecycle(SaleLifecycle.NOZZLE_RETURNED)
                elif self.sale_lifecycle in [SaleLifecycle.NOZZLE_LIFTED, SaleLifecycle.AUTHORIZED]:
                    self.transition_lifecycle(SaleLifecycle.ABORTED)

            return True
        return False

    def update_status(self, new_status: str, obs_time: float) -> bool:
        self.last_status_time = obs_time
        self.last_valid_frame_time = obs_time

        if self.observed_status != new_status:
            self.observed_status = new_status
            
            if new_status == "FILLING_COMPLETED":
                self.transition_lifecycle(SaleLifecycle.COMPLETED)
            elif new_status == "FILLING":
                self.transition_lifecycle(SaleLifecycle.FILLING)
            elif new_status == "AUTHORIZED":
                self.transition_lifecycle(SaleLifecycle.AUTHORIZED)
            elif new_status == "RESET":
                if self.sale_lifecycle in [SaleLifecycle.NOZZLE_RETURNED, SaleLifecycle.ABORTED, SaleLifecycle.COMPLETED]:
                    self.transition_lifecycle(SaleLifecycle.IDLE)
                
            return True
        return False